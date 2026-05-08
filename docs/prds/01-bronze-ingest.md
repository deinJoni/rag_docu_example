# PRD #1 — Bronze Ingest

**Status:** Implemented (reverse-documented 2026-05-07)
**Last updated:** 2026-05-07
**Owner:** Jonas
**Depends on:** —
**Unblocks:** PRD #2 (Silver transform), PRD #3 (Attachment text), PRD #4 (Gold embeddings)

## 1. Context

The repo runs a medallion-architecture RAG pipeline on Supabase. Bronze is the **raw-landing layer**: it reads the snapshot manifest from Supabase Storage, downloads every article JSON, and writes raw rows to `bronze.*` Postgres tables. Attachment **bytes** are not downloaded — only their metadata (path, kind, size, etag, mimetype) is registered, so silver/gold/PRD #3 can stream what they need on demand.

This PRD is a reverse-documentation of code that already shipped. It locks in the conventions that PRDs #2–#4 inherit: schema-per-layer, programmatic migrations, `run_id` shape, storage layout, idempotency model, retry posture.

### Corpus shape (verified against `docs-raw/source=climkit-helpdocs/snapshot=2026-05-07/`)

- **135 articles** — one JSON per article, each containing `multilingual[]` (de/en/fr/it) and category/tag metadata
- **168 files** (~337 MB) — mostly PDFs; technical datasheets and manuals
- **341 images** — referenced inline by article HTML
- **One `_manifest.json` per snapshot**, produced by an upstream migrate-to-bronze script (out of scope here); the loader treats it as the source of truth for "what files exist for this run"

### Locked decisions

| Decision | Choice | Rationale |
|---|---|---|
| Migration mechanism | **Programmatic** via `rag_shared.apply_migrations(conn)` at run-start, reading `packages/shared/src/rag_shared/migrations/*.sql` in lexical order | Pipeline jobs apply their own DDL; no Supabase CLI dependency in CI; idempotent (`CREATE … IF NOT EXISTS`). Diverges from PRD #3/#4 drafts which assume `supabase/migrations/` — see §10. |
| DB driver | **psycopg3** + `psycopg_pool.ConnectionPool` | Async-ready, supports `Jsonb`, pool sized to match worker count |
| Bucket | **`docs-raw`** (overridable via `SUPABASE_BUCKET`) | Single bucket per environment; layer separation is by storage prefix, not bucket |
| Storage layout | `<bucket>/source=<source>/snapshot=<YYYY-MM-DD>/{articles,files,images}/...` + `_manifest.json` at the snapshot prefix | Hive-style partitioning so snapshot-pruning and listing stay simple |
| Run identity | `run_id = f"{source}:{snapshot_date.isoformat()}"` (e.g. `"climkit-helpdocs:2026-05-07"`) | Stable, human-readable, serves as PK for `bronze.ingest_run` |
| Article-byte semantics | Download every article JSON; **etag short-circuit** if `(run_id, article_id)` already has matching etag | Re-runs are essentially free when nothing changed |
| Attachment-byte semantics | **Metadata-only.** Never download files/images at this layer | Bronze stays I/O-cheap; downstream consumers (PRD #3, future image-OCR) pull bytes when needed |
| Schema posture | One Postgres schema per layer (`bronze`, `silver`, `gold`); service-role only, no RLS exposure | RLS decisions defer to PRD #5 (Retrieval API) |

---

## 2. Architecture

### 2.1 Postgres schema: `bronze`

Three tables, all in `bronze`. DDL lives at `packages/shared/src/rag_shared/migrations/0001_bronze_schema.sql`:

```sql
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.ingest_run (
    run_id          text PRIMARY KEY,            -- "{source}:{YYYY-MM-DD}"
    source          text NOT NULL,
    snapshot_date   date NOT NULL,
    bucket          text NOT NULL,
    prefix          text NOT NULL,               -- 'source=…/snapshot=…/'
    manifest        jsonb,                       -- full _manifest.json
    counts          jsonb,                       -- {articles, files, images, total, ...}
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source, snapshot_date)
);

CREATE TABLE IF NOT EXISTS bronze.raw_article (
    run_id          text NOT NULL REFERENCES bronze.ingest_run (run_id) ON DELETE CASCADE,
    article_id      text NOT NULL,
    storage_path    text NOT NULL,
    payload         jsonb NOT NULL,              -- the article JSON, untouched
    size_bytes      bigint NOT NULL,
    etag            text,
    mimetype        text,
    loaded_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, article_id)
);

CREATE TABLE IF NOT EXISTS bronze.raw_attachment (
    run_id          text NOT NULL REFERENCES bronze.ingest_run (run_id) ON DELETE CASCADE,
    storage_path    text NOT NULL,               -- 'source=…/snapshot=…/files/<name>'
    kind            text NOT NULL CHECK (kind IN ('file','image')),
    name            text NOT NULL,               -- basename
    size_bytes      bigint NOT NULL,
    etag            text,
    mimetype        text,
    loaded_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, storage_path)
);
```

**Rationale:**
- `ingest_run` carries the **whole** manifest as `jsonb` and a small `counts` rollup. Cheap, and it lets verification queries inspect what the loader thought existed without re-fetching storage.
- `(source, snapshot_date)` UNIQUE on `ingest_run` enforces one row per logical snapshot, even though `run_id` is already the PK.
- `raw_article.payload` is **the JSON file, untouched**. No HTML stripping, no fanout to per-locale rows. That's silver's job.
- `raw_attachment` carries **no bytes**. The contract for downstream layers is "use `storage_path` to pull the blob when you need it" — exactly what PRD #3 does.
- `ON DELETE CASCADE` on `raw_article` and `raw_attachment` lets `DELETE FROM bronze.ingest_run WHERE run_id=…` cleanly drop a botched run.

### 2.2 Idempotency

| Op | Strategy |
|---|---|
| `ingest_run` write | `INSERT … ON CONFLICT (run_id) DO UPDATE` — refreshes `manifest`/`counts` on rerun |
| `raw_article` write | `INSERT … ON CONFLICT (run_id, article_id) DO UPDATE … SET loaded_at=now()` |
| `raw_attachment` write | `INSERT … ON CONFLICT (run_id, storage_path) DO UPDATE … SET loaded_at=now()` (executed via `executemany`) |
| Article download skip | Before downloading, read `bronze.raw_article.etag` for `(run_id, article_id)`. If it matches the manifest entry's etag → skip download + skip DB write. |
| Failure recovery | Per-article transaction. Failed articles don't poison the run; rerun retries them. Loader exits **2** if any failed or counts mismatch. |

### 2.3 Storage retry

`bronze/storage.py` wraps `client.storage.from_(bucket).download(path)` with a small retry loop:

- **3 attempts**, linear backoff (`0.5 × attempt` seconds)
- Heuristic 4xx detection on the exception string (`"404"`, `"403"`, `"401"`, `"not found"`, `"forbidden"`) → break early, don't retry
- Final failure raises `StorageDownloadError`; the worker catches it, marks the article as failed, and the orchestrator counts it

PRDs #3 ports this same shape verbatim for attachment downloads.

### 2.4 Concurrency

- `ThreadPoolExecutor(max_workers=workers)` for article downloads (default `--workers 8`).
- DB pool is sized to match: `get_pool(min_size=1, max_size=workers)`.
- Each article worker: read existing etag (one short txn) → if etag mismatch, download + upsert (second short txn). Two transactions per worker keeps row locks short.
- Attachment metadata: single connection, single `executemany`. No threads needed — there are no bytes to fetch.

---

## 3. Inputs

### 3.1 Storage manifest contract (`_manifest.json`)

Read from `<bucket>/<prefix>_manifest.json` and parsed into `bronze.manifest.Manifest`:

```python
class ManifestEntry(BaseModel):
    path: str                               # 'source=…/snapshot=…/articles/<id>.json' etc
    kind: Literal['article', 'file', 'image']
    size: int
    mimetype: str | None = None
    etag: str | None = None

class ManifestCounts(BaseModel):
    articles: int = 0
    files: int = 0
    images: int = 0
    total: int = 0
    moved_ok: int | None = None
    moved_fail: int | None = None

class Manifest(BaseModel):
    ingest_run_id: str
    source: str
    snapshot_date: date
    ingested_at: str
    origin: dict[str, Any]                  # provenance from migrate-to-bronze script
    counts: ManifestCounts
    files: list[ManifestEntry]
```

`Manifest.by_kind()` partitions `files` into `{'article': [...], 'file': [...], 'image': [...]}`.

### 3.2 Article JSON shape (subset relied on)

The loader writes `payload` opaquely, but two fields are read defensively at this layer:

- `article_id` — fallback derivation from path basename (`<id>.json`) if missing in payload
- `mimetype` — taken from manifest entry, not payload

Everything else (multilingual body, category_path, tags, slug, url) is left for silver.

### 3.3 Settings (`rag_shared.get_settings()`)

```python
class Settings(BaseSettings):
    supabase_url: str           # SUPABASE_URL
    supabase_secret_key: str    # SUPABASE_SECRET_KEY
    supabase_bucket: str        # SUPABASE_BUCKET, e.g. 'docs-raw'
    database_url: str           # DATABASE_URL (transaction pooler, port 6543)
```

`.env` and `apps/pipeline/.env` are both consulted (pydantic-settings `env_file` tuple). `@lru_cache` ensures one read per process.

---

## 4. Module layout

```
packages/shared/src/rag_shared/
  __init__.py                                 (re-exports apply_migrations, get_pool, get_settings, get_supabase, …)
  db.py                                       (ConnectionPool + apply_migrations)
  env.py                                      (Settings)
  supabase.py                                 (cached Supabase client)
  migrations/
    0001_bronze_schema.sql                    (DDL from §2.1)

apps/pipeline/src/pipeline/
  __main__.py                                 (LAYERS dispatch: bronze|silver|gold)
  bronze/
    __init__.py                               (re-exports run, run_bronze)
    load.py                                   (orchestrator: manifest → ingest_run → articles → attachments → verify)
    db.py                                     (upsert helpers, count_loaded)
    manifest.py                               (Manifest pydantic models + read_manifest)
    storage.py                                (download_bytes/download_json with retry)
```

### 4.1 Reusable utilities surfaced here

These are introduced by this PRD and consumed by silver/gold/PRD #3:

- **`rag_shared.apply_migrations(conn)`** — applies `migrations/*.sql` in lexical order; idempotent. Both bronze and silver call it at run-start.
- **`rag_shared.get_pool(min_size, max_size)`** — `@lru_cache`'d psycopg3 pool. Default 1/8.
- **`rag_shared.get_settings()`** — `@lru_cache`'d `Settings`.
- **`rag_shared.get_supabase()`** — cached storage client.
- **`bronze.storage.download_bytes` / `download_json`** — the retry pattern PRD #3 ports for attachment downloads.

---

## 5. CLI contract

```
$ uv run pipeline bronze --source climkit-helpdocs --snapshot 2026-05-07
[bronze] applying migrations
[bronze]   applied: 0001_bronze_schema.sql
[bronze] reading manifest docs-raw/source=climkit-helpdocs/snapshot=2026-05-07/_manifest.json
[bronze] upserting ingest_run climkit-helpdocs:2026-05-07
[bronze] loading 135 articles (8 workers)...
[bronze] articles: ok=135 skipped=0 failed=0 in 12.3s
[bronze] loading 509 attachments (metadata only)...
[bronze] attachments: ok=509 in 0.4s
[bronze] done. articles=135 files=168 images=341 total=644
```

`--source` and `--snapshot` (`YYYY-MM-DD`) are required. `--workers` defaults to 8.

Exit codes:
- `0` — all articles loaded, counts match manifest
- `1` — argparse / settings failure
- `2` — partial load (failed articles or count mismatch). Rerun to retry.

---

## 6. Pipeline flow (`pipeline/bronze/load.py::run_bronze`)

1. Resolve `bucket = settings.supabase_bucket`, `prefix = f"source={source}/snapshot={snapshot}/"`, `run_id = f"{source}:{snapshot}"`.
2. Open the shared psycopg pool sized to `--workers`. Apply migrations.
3. Download `<bucket>/<prefix>_manifest.json`, parse to `Manifest`. Warn (don't fail) if `manifest.source` / `snapshot_date` disagree with CLI args.
4. Upsert one `bronze.ingest_run` row carrying the full manifest + counts.
5. Partition manifest entries by kind. For **articles** (parallel, `ThreadPoolExecutor`):
   1. Read existing etag for `(run_id, article_id)` (short txn, commit immediately).
   2. If etag matches manifest → return `did_work=False`.
   3. Else download JSON via `storage.download_json` (3-attempt retry).
   4. Resolve `article_id` from payload (fallback: basename).
   5. Upsert `bronze.raw_article` (second short txn).
   6. Catch any exception → return `(path, False, str(err))`. Aggregator collects failures.
6. For **attachments** (file + image): one connection, `executemany` over `bulk_upsert_attachments`.
7. Verify: `count_loaded(run_id)` vs manifest counts. If failures > 0 or any mismatch, log and exit 2.
8. Print summary line.

### Failure semantics

- Per-article failure → captured, logged in summary (`failed=N`), first 10 printed to stderr. Run exits 2; rerun replays.
- Storage download retries exhausted → counts as a per-article failure, not a global crash.
- Manifest source/date mismatch with CLI args → warning only (the manifest is authoritative for counts; CLI args drive `run_id`).
- Migration failure → fail fast before any data writes.

---

## 7. Dependencies

`apps/pipeline/pyproject.toml`:

```toml
dependencies = [
  "rag-shared",
  "supabase>=2.10.0",
  "pydantic>=2.10.0",
  "pydantic-settings>=2.6.0",
  "beautifulsoup4>=4.12.0",                # silver, listed here for completeness
  "markdownify>=0.13.0",                    # silver
]
```

`packages/shared/pyproject.toml`:

```toml
dependencies = [
  "supabase>=2.10.0",
  "pydantic>=2.10.0",
  "pydantic-settings>=2.6.0",
  "psycopg[binary,pool]>=3.2",
]
```

No system-level deps (no native libs). Bronze adds nothing beyond `rag-shared`'s `psycopg` and the existing `supabase` client.

---

## 8. Verification

End-to-end smoke that confirms the layer is working:

1. **Schema present:** `select tablename from pg_tables where schemaname='bronze';` → 3 rows (`ingest_run`, `raw_article`, `raw_attachment`).
2. **Cold run:** `uv run pipeline bronze --source climkit-helpdocs --snapshot 2026-05-07` → exits 0, summary line shows `articles=135 files=168 images=341`.
3. **Manifest stored:** `select counts from bronze.ingest_run where run_id='climkit-helpdocs:2026-05-07';` → matches `{articles:135, files:168, images:341, ...}`.
4. **Idempotent rerun:** rerun the same CLI → second run shows `skipped=135` (every article etag matched). Article download is essentially zero work.
5. **Etag invalidation:** `update bronze.raw_article set etag='stale' where article_id=(select article_id from bronze.raw_article limit 1);` then rerun → exactly 1 article re-downloaded.
6. **Attachment metadata only:** `select sum(size_bytes) from bronze.raw_attachment;` ≈ 337 MB; **no rows have payloads**, no storage download for attachments occurred.
7. **Cascade safety:** `delete from bronze.ingest_run where run_id='climkit-helpdocs:2026-05-07';` → `raw_article` + `raw_attachment` rows for that run are removed automatically.
8. **Failure path:** kill the network mid-run → loader exits 2, partial rows persist, rerun completes the missing ones.

---

## 9. Out of scope (explicitly deferred)

- **HTML cleaning / multilingual fanout / link parsing** → PRD #2.
- **Attachment-byte download + text extraction** → PRD #3.
- **Embedding / chunking / pgvector** → PRD #4.
- **Manifest production** — assumed already on storage; written by an upstream migrate-to-bronze script that is not part of the pipeline.
- **Multi-source orchestration** — only `climkit-helpdocs` runs today; the layer accepts any `--source` but no scheduler picks more than one.
- **RLS / anon exposure** — service-role only at every layer until PRD #5.
- **Soft-delete / history** — `ON DELETE CASCADE` and TRUNCATE-style replay are good enough at this scale.

---

## 10. Open questions / known divergences

1. **Migration mechanism vs PRD #3/#4 drafts.** PRDs #3 and #4 describe migrations at `supabase/migrations/<timestamp>_<name>.sql` applied via `supabase db push`. Bronze (and silver) instead apply DDL **programmatically** at run-start from `packages/shared/src/rag_shared/migrations/*.sql`. PRDs #3/#4 will need to either (a) follow the programmatic convention and add `0003_*.sql` / `0004_*.sql` to the same dir, or (b) we keep two migration paths. **Recommendation:** stick with programmatic for pipeline-owned schemas; reserve `supabase/migrations/` for app-facing tables (RLS-bound, used by PRD #5).
2. **Counts column on `raw_attachment`.** No per-mimetype rollup is precomputed. If we ever want fast "how many PDFs in this run" without scanning, add a materialized view. Not worth it at 168 files.
3. **Article `mimetype` taken from manifest, not payload.** If a future source produces JSON with embedded mimetype hints that disagree, we'll need to decide which wins. Current code prefers manifest.
4. **`Manifest.origin: dict`** is opaque. It carries provenance from the migrate-to-bronze script but no code reads it. Consider tightening to a model once a second source exists.
5. **No telemetry.** Failures are printed to stderr; there's no metrics emission. Acceptable at one source / one snapshot per day; revisit if cron'd.

---

## 11. Critical files

### Implementation lives in:

- `/Users/jonas/Documents/Pracht/rag_docu_example/packages/shared/src/rag_shared/migrations/0001_bronze_schema.sql`
- `/Users/jonas/Documents/Pracht/rag_docu_example/packages/shared/src/rag_shared/db.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/packages/shared/src/rag_shared/env.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/packages/shared/src/rag_shared/supabase.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/packages/shared/src/rag_shared/__init__.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/__main__.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/bronze/__init__.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/bronze/load.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/bronze/db.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/bronze/manifest.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/bronze/storage.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/.env.example`

### Config:

- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/pyproject.toml`
- `/Users/jonas/Documents/Pracht/rag_docu_example/packages/shared/pyproject.toml`
