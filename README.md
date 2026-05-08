# rag-docu-example

A medallion-architecture RAG ingestion pipeline on Supabase. The pipeline reads a snapshot of help-center articles from Supabase Storage and lands it through three layers — **bronze** (raw), **silver** (cleaned + per-locale + attachment text), and **gold** (chunks + embeddings) — into Postgres.

## Architecture

### Medallion layers

```
Supabase Storage (docs-raw bucket)        Postgres (Supabase)
  source=<src>/snapshot=<date>/             bronze.ingest_run
    _manifest.json          ──────────►     bronze.raw_article
    articles/<id>.json                      bronze.raw_attachment
    files/<name>                                 │
    images/<name>                                ▼
                                            silver.article
                                            silver.article_translation  (de/en/fr/it)
                                            silver.attachment
                                            silver.article_attachment_ref
                                            silver.attachment_text
                                                 │
                                                 ▼
                                            gold.chunk           (planned)
                                            gold.chunk_embedding (planned)
```

- **Bronze** — reads `_manifest.json`, downloads each article JSON in parallel (etag short-circuit on re-run), registers every file/image as metadata only. Bytes are pulled on demand by downstream layers, never staged here.
- **Silver — `build` stage** — replays the chosen bronze run into `silver.*`: HTML → plain text + markdown, multilingual fanout into `article_translation`, plus a `article_attachment_ref` graph (inline `<img>`, body links, filename-prefix matches). TRUNCATE-and-rewrite per run.
- **Silver — `attachment-text` stage** — streams attachment bytes from storage, dispatches by mimetype/suffix to a per-format extractor (pypdf with pdfplumber fallback, python-docx, stdlib csv), upserts one row per `(attachment, extractor_version)` into `silver.attachment_text`. Idempotent via an anti-join on the supported-version list — bumping a `HEURISTIC_VERSION` re-runs every affected attachment.
- **Gold** — not implemented yet (see [docs/prds/04-gold-embeddings.md](docs/prds/04-gold-embeddings.md)).

Schemas are programmatically applied at run-start from [packages/shared/src/rag_shared/migrations/](packages/shared/src/rag_shared/migrations/) — DDL is `CREATE … IF NOT EXISTS`, so re-running is free. No Supabase CLI dependency.

### Repository layout

```
apps/pipeline/        # Python CLI: `pipeline bronze|silver|gold`
  src/pipeline/
    __main__.py         # layer dispatcher
    bronze/             # load.py, manifest.py, storage.py, db.py
    silver/             # build.py + attachment_text/ (runner, extractors, db)
    gold/               # stub
  scripts/verify_attachment_text.py   # post-run aggregate report
  tests/                # extractor unit tests (no DB, no network)
  .env.example

packages/shared/      # rag_shared lib
  src/rag_shared/
    env.py              # pydantic-settings loader
    db.py               # psycopg3 ConnectionPool + apply_migrations()
    supabase.py         # cached Client
    migrations/*.sql    # 0001 bronze, 0002 silver, 0003 attachment_text

docs/prds/            # design docs 01–04 (01–03 implemented, 04 draft)
data/bronze/          # local inventory.jsonl (gitignored otherwise)
pyproject.toml        # uv workspace root (Python tooling: ruff, pyright)
package.json          # pnpm + turbo (kept for future JS apps)
```

The repo is a uv workspace (Python) inside a pnpm/turbo workspace (JS). Right now everything that does work is Python under `apps/pipeline` and `packages/shared`; the Node side is scaffolding for future apps.

## Current state

| Layer | Stage | Status | PRD |
|---|---|---|---|
| bronze | load | shipped | [#1](docs/prds/01-bronze-ingest.md) |
| silver | build | shipped | [#2](docs/prds/02-silver-transform.md) |
| silver | attachment-text | shipped (PDF/DOCX/CSV; image-only PDFs flagged `needs_ocr`) | [#3](docs/prds/03-attachment-text-extraction.md) |
| gold | embeddings | not implemented (CLI prints a stub message) | [#4](docs/prds/04-gold-embeddings.md) |

Verified corpus: 135 articles × 4 locales, 168 files (~337 MB), 341 images.

## Setup

Prereqs: Python 3.12 (`.python-version`), [uv](https://docs.astral.sh/uv/), Node 24 + pnpm 10 (only if you plan to add JS apps), a Supabase project with the snapshot already uploaded to storage.

```bash
# Python deps (workspace install for both apps/pipeline and packages/shared)
uv sync

# JS scaffolding (optional today)
pnpm install
```

Configure credentials in `apps/pipeline/.env` (or a repo-root `.env`; both are searched):

```bash
cp apps/pipeline/.env.example apps/pipeline/.env
# fill in SUPABASE_URL, SUPABASE_SECRET_KEY, SUPABASE_BUCKET, DATABASE_URL
```

`DATABASE_URL` should be the Supabase **transaction pooler** connection string (port 6543) — see the comment in [.env.example](apps/pipeline/.env.example).

## Usage

The single entry point is the `pipeline` CLI installed by the `apps/pipeline` package. Migrations are applied automatically on every layer run.

### Bronze — land a snapshot

```bash
uv run pipeline bronze --source climkit-helpdocs --snapshot 2026-05-07 [--workers 8]
```

Reads `<bucket>/source=<source>/snapshot=<date>/_manifest.json`, parallel-downloads article JSON, registers attachment metadata, verifies counts vs. manifest. Re-runs are idempotent and skip articles whose etag still matches.

### Silver — clean + extract

```bash
# Both stages in order (default). --snapshot defaults to the latest bronze run for --source.
uv run pipeline silver --source climkit-helpdocs

# Or run one stage at a time:
uv run pipeline silver --stage build
uv run pipeline silver --stage attachment-text
```

`build` TRUNCATEs and rewrites `silver.*` from the chosen bronze run. `attachment-text` only extracts attachments that don't yet have a row at a currently-supported `extractor_version`.

### Gold

```bash
uv run pipeline gold
# [gold] not implemented yet
```

### Verifying attachment-text results

```bash
uv run python apps/pipeline/scripts/verify_attachment_text.py
```

Read-only aggregate report against `silver.attachment_text`: per-status counts, distinct extractor versions, full lists of `needs_ocr` and `error` rows, sample `ok` snippets, and downstream-readiness count.

### Tests

```bash
uv run pytest apps/pipeline/tests
```

Pure extractor tests — no DB, no Supabase, no network. Fixtures live in [apps/pipeline/tests/fixtures/](apps/pipeline/tests/fixtures/).

### Lint / typecheck

```bash
uv run ruff check .
uv run pyright
```

Pyright is configured for `strict` mode against `apps/pipeline/src` and `packages/shared/src`.

## Design references

The PRDs in [docs/prds/](docs/prds/) are the source of truth for layer contracts and locked decisions. Start with PRD #1 — it documents conventions inherited by every later layer (run-id shape, storage layout, migration mechanism, idempotency model, schema-per-layer).
