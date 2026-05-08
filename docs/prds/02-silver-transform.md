# PRD #2 — Silver Transform

**Status:** Implemented (reverse-documented 2026-05-07)
**Last updated:** 2026-05-07
**Owner:** Jonas
**Depends on:** PRD #1 (Bronze ingest — reads `bronze.raw_article` + `bronze.raw_attachment`)
**Unblocks:** PRD #3 (Attachment text — consumes `silver.attachment`), PRD #4 (Gold embeddings — consumes `silver.article_translation`)

## 1. Context

Silver is the **cleaned, structured layer**. It reads the opaque per-article JSON that PRD #1 lands in `bronze.raw_article`, fans each article out into per-locale rows, strips/normalises the HTML body into both plaintext (for embedding) and markdown (for retrieval display), catalogues every attachment as a first-class row, and resolves the cross-references between articles and attachments.

This PRD is a reverse-documentation of code that already shipped. It pins the contracts that PRDs #3 and #4 depend on:

- `silver.article_translation(article_id, language_code, body_text, ...)` — the input to gold's chunker
- `silver.attachment(storage_path, kind, mimetype, ...)` — the input to PRD #3's extractor
- `silver.article_attachment_ref(...)` — which attachments belong to which article and how (inline image / body link / filename prefix)

PRDs #3 and #4 **were drafted with slightly different silver contracts in mind** (see §10). This document describes the actual schema; reconciling the drafts is an open item for those PRDs.

### Corpus shape (verified against `climkit-helpdocs:2026-05-07`)

- **135 articles** in `bronze.raw_article` → **540 `silver.article_translation` rows** (one per article × locale, locales `de`/`en`/`fr`/`it`)
- **509 attachments** in `bronze.raw_attachment` → **509 `silver.attachment` rows** (168 files + 341 images, kept verbatim)
- **`silver.article_attachment_ref` rows**: a few thousand, split across three `ref_kind` values (`inline_img`, `body_link`, `filename_prefix`)
- Per-locale plaintext: median ~125 tokens, max ~900 tokens (consumed by PRD #4 directly)

### Locked decisions

| Decision | Choice | Rationale |
|---|---|---|
| Replay model | **Single-source `TRUNCATE … CASCADE` + INSERT** for every run | Silver is fully deterministic from a given bronze run. No partial-update/conflict-handling logic to maintain. |
| Source selection | `--source` (default `climkit-helpdocs`) + `--snapshot` (default: latest bronze run for that source) | Operator-friendly: `pipeline silver` "just runs" against whatever bronze produced last. |
| HTML pipeline | **BeautifulSoup + markdownify**: produces `body_text` (anchors flattened, images removed, paragraphs collapsed) **and** `body_markdown` (headings/links/images preserved; `<div class="tip-callout">` rewritten to `> NOTE: …` blockquotes). Original `body_html` stored too. | `body_text` for embedding, `body_markdown` for human-facing retrieval display, `body_html` as escape hatch. Three columns is cheap; debating which to embed isn't. |
| Locales | **Embed every locale separately** as its own `silver.article_translation` row | Native-language queries hit native-language docs first. PRD #4 inherits this. |
| Primary language | Detected from canonical URL (`/l/<lang>/…` regex), default `fr` | Matches HelpDocs URL convention; `fr` is the editorial primary. |
| Attachment dedup | None at this layer — `silver.attachment` is keyed by `storage_path` (already unique within a run) | PRD #3 was drafted with content-hash dedup in mind; the actual table doesn't carry one. See §10. |
| Cross-article refs | Detected and **counted only** (logged in summary as `cross_article_links_seen=N`) — no FK row is written | Out of scope for retrieval; revisit if PRD #5 wants in-app deep links. |

---

## 2. Architecture

### 2.1 Postgres schema: `silver`

DDL lives at `packages/shared/src/rag_shared/migrations/0002_silver_schema.sql`:

```sql
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.article (
    article_id          text PRIMARY KEY,
    run_id              text NOT NULL REFERENCES bronze.ingest_run (run_id),
    source              text NOT NULL,
    snapshot_date       date NOT NULL,
    primary_language    text NOT NULL,                              -- 'de'|'en'|'fr'|'it'
    category_id         text,
    category_path       jsonb,                                      -- HelpDocs breadcrumb
    is_published        boolean NOT NULL,
    is_private          boolean NOT NULL,
    helpdocs_version    int,
    canonical_url       text NOT NULL,
    tags                jsonb,
    transformed_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.article_translation (
    article_id          text NOT NULL REFERENCES silver.article ON DELETE CASCADE,
    language_code       text NOT NULL,                              -- 'de'|'en'|'fr'|'it'
    title               text NOT NULL,
    description         text,
    short_version       text,
    slug                text NOT NULL,
    url                 text NOT NULL,
    relative_url        text NOT NULL,
    version_number      int,
    body_html           text NOT NULL,
    body_text           text NOT NULL,                              -- ← gold embeds this
    body_markdown       text NOT NULL,                              -- ← retrieval display
    text_length         int NOT NULL,
    PRIMARY KEY (article_id, language_code)
);
CREATE INDEX IF NOT EXISTS article_translation_lang_idx
    ON silver.article_translation (language_code);

CREATE TABLE IF NOT EXISTS silver.attachment (
    storage_path            text PRIMARY KEY,
    run_id                  text NOT NULL REFERENCES bronze.ingest_run (run_id),
    kind                    text NOT NULL CHECK (kind IN ('file','image')),
    name                    text NOT NULL,
    article_id_from_name    text,                                   -- regex-derived hint
    size_bytes              bigint NOT NULL,
    mimetype                text,
    etag                    text,
    transformed_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS attachment_article_id_idx
    ON silver.attachment (article_id_from_name);

CREATE TABLE IF NOT EXISTS silver.article_attachment_ref (
    article_id      text NOT NULL REFERENCES silver.article ON DELETE CASCADE,
    storage_path    text NOT NULL REFERENCES silver.attachment,
    language_code   text NOT NULL,                                  -- '*' for filename_prefix
    ref_kind        text NOT NULL CHECK (ref_kind IN ('inline_img','body_link','filename_prefix')),
    PRIMARY KEY (article_id, storage_path, language_code, ref_kind)
);
```

**Rationale:**
- **Two-table article model.** `silver.article` is locale-agnostic metadata (category, tags, primary language, run identity). `silver.article_translation` is the per-locale cleaned content. Joining on `article_id` is cheap; embedding-time queries only ever need the translation table.
- **`storage_path` as PK on `silver.attachment`.** Inherited from `bronze.raw_attachment`'s `(run_id, storage_path)` PK — once silver is single-snapshot per run, `storage_path` alone is unique. PRD #3's draft assumes a `bigserial id` and FKs into it; that's a small migration to add later if needed (see §10).
- **`article_id_from_name`.** A regex (`^([a-z0-9]{10})-\d+-`) parses the leading 10-char article ID out of HelpDocs-exported attachment filenames. When it matches a real `article_id`, we record a `filename_prefix` ref. Cheap heuristic that pulls in attachments referenced by filename convention rather than HTML link.
- **`ref_kind` enumerates the three discovery paths**: `inline_img` (in an `<img src>`), `body_link` (in an `<a href>`), `filename_prefix` (regex hit on attachment basename). All three are lossy individually; together they pull in essentially every used attachment.
- **`article_attachment_ref.language_code='*'` for `filename_prefix`** — filename-derived links don't carry locale context; the wildcard avoids exploding refs across all four locales.

### 2.2 Idempotency

| Op | Strategy |
|---|---|
| Whole-run replay | **`TRUNCATE silver.article_attachment_ref, silver.article_translation, silver.attachment, silver.article CASCADE`** (in dependency order), then `INSERT` everything from the chosen bronze run. |
| Re-running with the same bronze snapshot | Bit-for-bit identical output (BeautifulSoup is deterministic; the SQL is one ordered scan + bulk inserts). |
| Switching snapshots | Pass `--snapshot YYYY-MM-DD` (or omit to take latest); same TRUNCATE+INSERT. There is **no historical retention** at the silver layer — only one snapshot's worth of data lives here at a time. |
| Failure recovery | Single transaction wraps the bulk inserts; partial state is impossible. On exception, silver stays empty until a successful rerun. |

The trade-off is plain: silver re-emits ~3000 rows from scratch every time, which takes a few seconds. In exchange, every silver reader (PRDs #3, #4, #5) sees a clean, internally consistent view with no version skew.

### 2.3 HTML pipeline (`silver/html_clean.py`)

Two pure functions, both sharing a `_strip_unwanted` step that decomposes `<script>`, `<style>`, `<iframe>` and HTML comments.

**`to_text(html)` — for embedding**

1. Strip unwanted tags + comments.
2. Decompose every `<img>`.
3. Replace every `<a>` with its inner text.
4. `soup.get_text("\n")`.
5. Collapse whitespace, group blank-line-separated runs into paragraphs joined by `"\n\n"`.

The output is plaintext with paragraph structure intact and no markup tokens — exactly what an embedding model wants.

**`to_markdown(html)` — for retrieval display**

1. Strip unwanted tags + comments.
2. Rewrite every `<div class="tip-callout">` to `<blockquote>` prefixed with `<strong>NOTE: </strong>`. (HelpDocs-specific custom-callout convention.)
3. `markdownify` with `heading_style="ATX"`, `bullets="-"`, stripping the same tag list.
4. Collapse runs of 3+ newlines to 2.

### 2.4 Link parser (`silver/link_parser.py`)

```python
@dataclass
class BodyRefs:
    inline_imgs: list[str]      # basenames of <img src> matching attachment_names
    body_links: list[str]       # basenames of <a href> matching attachment_names
    cross_articles: list[str]   # 10-char article IDs from /l/<lang>/article/<id>-…
    external_count: int         # everything else (logged, not persisted)
```

`extract_refs(html, attachment_names)`:
1. Walk `<img>`: `_basename(src)`. If in `attachment_names` → `inline_imgs`.
2. Walk `<a>`: skip `mailto:` / `tel:`. Match `^/l/([a-z]{2})/article/([a-z0-9]{10})-` → `cross_articles`. Else `_basename(href)`: if in `attachment_names` → `body_links`, else bump `external_count`.

Notes:
- `_basename` URL-decodes the path so `Photo%20du%2026.10.2019.png` matches.
- `attachment_names: set[str]` is built once from `silver.attachment.name`; lookup is O(1) per ref.
- `cross_articles` is counted but never written. PRD #5 may revive it.

---

## 3. Inputs

### 3.1 Bronze contract (PRD #1 produces)

```sql
bronze.ingest_run(run_id, source, snapshot_date, …)
bronze.raw_article(run_id, article_id, payload jsonb, …)
bronze.raw_attachment(run_id, storage_path, kind, name, size_bytes, mimetype, etag, …)
```

Silver picks a run with either:
- `latest_run_for_source(source)` → `(run_id, source, snapshot_date)` ordered by `snapshot_date DESC LIMIT 1`, or
- `find_run(source, snapshot_date)` → exact match.

If no row matches, `run_silver` raises `RuntimeError(... — run \`pipeline bronze\` first)`. Silver never runs against an empty bronze.

### 3.2 `payload` shape relied on (subset of HelpDocs export)

Silver reads these fields from `bronze.raw_article.payload`:

```jsonc
{
  "url": "https://help.…/l/fr/article/<id>-…",   // primary_language regex
  "category_id": "…",
  "category_path": [...],
  "is_published": true,
  "is_private": false,
  "version_number": 7,
  "tags": [...],
  "multilingual": [
    {
      "language_code": "de",
      "title": "...",
      "description": "...",
      "short_version": "...",
      "slug": "...",
      "url": "...",
      "relative_url": "...",
      "version_number": 7,
      "body": "<ol>…</ol>"
    },
    // … one entry per locale
  ]
}
```

If `payload.multilingual` is missing or empty, the article gets a `silver.article` row but no translations. (No warning logged — the situation is rare and visible from `count_silver`.)

---

## 4. Module layout

```
packages/shared/src/rag_shared/migrations/
  0002_silver_schema.sql                      (DDL from §2.1)

apps/pipeline/src/pipeline/silver/
  __init__.py                                 (re-exports run, run_silver)
  build.py                                    (orchestrator: pick run → load bronze → transform → bulk insert)
  db.py                                       (latest_run_for_source / find_run / fetch_bronze_* / bulk inserts / count_silver)
  html_clean.py                               (to_text + to_markdown)
  link_parser.py                              (extract_refs returning BodyRefs)
  transform.py                                (parse_article_id_from_name + primary_language_from_url regexes)
```

### 4.1 Reused from PRD #1

- `rag_shared.apply_migrations(conn)` — runs `0001_*` + `0002_*` in order.
- `rag_shared.get_pool()` — same psycopg3 pool as bronze.
- No Supabase storage client needed at this layer (silver pulls from Postgres only).

### 4.2 New runtime dependencies

`apps/pipeline/pyproject.toml` adds:

```toml
"beautifulsoup4>=4.12.0",
"markdownify>=0.13.0",
```

Both pure-Python, MIT/BSD licensed.

---

## 5. CLI contract

```
$ uv run pipeline silver --source climkit-helpdocs --snapshot 2026-05-07
[silver] applying migrations
[silver]   applied: 0001_bronze_schema.sql, 0002_silver_schema.sql
[silver] using bronze run_id=climkit-helpdocs:2026-05-07 source=climkit-helpdocs snapshot=2026-05-07
[silver]   bronze: articles=135 attachments=509
[silver] writing: articles=135 translations=540 attachments=509 refs=1247
[silver] done. counts={'article': 135, 'article_translation': 540, 'attachment': 509, 'article_attachment_ref': 1247} \
        refs_breakdown=(inline_img=812, body_link=143, filename_prefix=292) \
        cross_article_links_seen=88 external_links_seen=204 elapsed=4.1s
```

Defaults:
- `--source` defaults to `climkit-helpdocs` (only source today).
- `--snapshot` is **optional**; if omitted, takes the latest bronze run for `--source`. (Diverges from bronze, where `--snapshot` is required — silver is a downstream step where "latest" is the common operator intent.)

Exit codes:
- `0` — all writes committed
- `1` — argparse / settings failure
- non-zero with traceback if the chosen bronze run doesn't exist (`RuntimeError`)

---

## 6. Pipeline flow (`pipeline/silver/build.py::run_silver`)

1. Open the shared psycopg pool. `apply_migrations(conn)` (idempotent).
2. Resolve target run via `latest_run_for_source` or `find_run`. Fail loud if nothing found.
3. Fetch `bronze.raw_article(article_id, payload)` and `bronze.raw_attachment(storage_path, kind, name, size_bytes, mimetype, etag)` for the chosen `run_id`.
4. Build `attachment_name_to_path` and `attachment_names: set[str]` for fast link resolution.
5. **Per article:**
   1. Build a `silver.article` row (`primary_language` via URL regex; `category_path`/`tags` wrapped in `Jsonb`).
   2. Per `multilingual[]` entry:
      - Build `silver.article_translation` row: `body_text = to_text(body_html)`, `body_markdown = to_markdown(body_html)`, `text_length = len(body_text)`.
      - `extract_refs(body_html, attachment_names)`:
        - Each `inline_img` basename → `(article_id, storage_path, lang, 'inline_img')` ref.
        - Each `body_link` basename → `(article_id, storage_path, lang, 'body_link')` ref.
        - Increment `cross_seen` / `external_seen` counters (display only).
6. **Per attachment:**
   1. Build `silver.attachment` row.
   2. `parse_article_id_from_name(name)` → if match and that `article_id` is in the article set, append a `(article_id, storage_path, '*', 'filename_prefix')` ref.
7. Open one transaction:
   - `truncate_silver(conn)` — TRUNCATE all four tables CASCADE.
   - `bulk_insert_articles` → `bulk_insert_translations` → `bulk_insert_attachments` → `bulk_insert_refs`.
   - `commit`.
   - `count_silver(conn)` for the summary line.
8. Print summary with row counts and ref-kind breakdown.

### Failure semantics

- **Bronze not present** for the requested `(source, snapshot)` → `RuntimeError`, no DDL run effects, no writes.
- **One article's HTML blows up BeautifulSoup** → exception propagates and aborts the run **before** any TRUNCATE. (Silver state remains whatever the last successful run produced.) Bug fix path: investigate the offending article; rerun.
- **Bulk insert constraint failure** (e.g. dangling FK in `article_attachment_ref`) → transaction rolls back; silver stays empty until the bug is fixed. We have not seen this in practice — refs are only emitted when both sides are known to be present in the same run.

---

## 7. Dependencies

`apps/pipeline/pyproject.toml`:

```toml
dependencies = [
  "rag-shared",
  "supabase>=2.10.0",
  "pydantic>=2.10.0",
  "pydantic-settings>=2.6.0",
  "beautifulsoup4>=4.12.0",
  "markdownify>=0.13.0",
]
```

No system-level dependencies. No network calls (silver is DB-only). No LLMs.

---

## 8. Verification

End-to-end smoke that confirms the layer is working:

1. **Schema present:** `select tablename from pg_tables where schemaname='silver';` → 4 rows.
2. **Cold run:** `uv run pipeline silver --source climkit-helpdocs` → exits 0, summary line lists `article=135 article_translation=540 attachment=509`.
3. **Locale split:** `select language_code, count(*) from silver.article_translation group by 1;` → 4 rows (`de`/`en`/`fr`/`it`), each ~135.
4. **Plaintext sanity:** `select substring(body_text, 1, 200) from silver.article_translation order by random() limit 3;` — readable text, no `<…>` tags, no entity gibberish.
5. **Markdown sanity:** `select body_markdown from silver.article_translation where body_markdown ilike '%> **NOTE:**%' limit 1;` — confirms tip-callout rewriting.
6. **Attachment coverage:** `select kind, count(*) from silver.attachment group by 1;` → `file=168, image=341`.
7. **Ref breakdown:** `select ref_kind, count(*) from silver.article_attachment_ref group by 1;` — non-zero in all three buckets (`inline_img`, `body_link`, `filename_prefix`).
8. **Idempotent rerun:** rerun with same args → identical row counts; `transformed_at` advances but content does not.
9. **Snapshot pin:** rerun with `--snapshot 2026-05-07` explicitly → same as default (latest path).
10. **Cascade safety:** `delete from silver.article where article_id=(select article_id from silver.article limit 1);` → translations and refs for that article cascade out; attachments stay (not FK'd in that direction).

---

## 9. Out of scope (explicitly deferred)

- **Attachment text extraction** → PRD #3 (consumes `silver.attachment`).
- **Embedding / chunking / pgvector** → PRD #4 (consumes `silver.article_translation`).
- **Cross-article ref persistence** — counted but not written. Revisit if PRD #5 wants in-app deep links.
- **Multi-snapshot retention** — silver is single-snapshot; switching `--snapshot` overwrites. If we ever want side-by-side comparisons, add a `run_id` PK component (already on each table) and stop TRUNCATE-ing.
- **Source-hash / `source_hash` column on translations** — PRD #4's draft assumes silver provides one; current silver does not. Either gold computes it inline (simpler) or silver adds a generated column (cleaner). See §10.
- **Surrogate `bigserial id` on `silver.attachment`** — PRD #3's draft expects one; current silver uses `storage_path` as PK. See §10.
- **Multi-source orchestration** — only `climkit-helpdocs` runs today.
- **RLS / anon exposure** — service-role only until PRD #5.

---

## 10. Open questions / known divergences

These are the deltas between **what silver actually emits** and what **PRDs #3 and #4 (drafts) assume**. They need to be resolved before #3 / #4 can be implemented as written.

1. **`silver.attachment` shape vs PRD #3.**
   - PRD #3 §3.1 expects: `id bigserial PK`, plus `unique (run_id, storage_path)`, plus `sha256` for dedup.
   - Actual: `storage_path` is the PK; no `id`, no `sha256`.
   - **Resolution path:** either (a) PRD #3 keys its `attachment_text.attachment_path text not null` on `storage_path` directly (one-line schema change in PRD #3, no silver changes), or (b) silver adds a `bigserial id` column in a `0003_silver_attachment_id.sql` migration. (a) is simpler and we recommend it.
2. **`silver.article_translation.source_hash` vs PRD #4.**
   - PRD #4 §3.1 expects silver to compute and store `source_hash = sha256(title || '\n\n' || body_text)`.
   - Actual: no such column.
   - **Resolution path:** either (a) gold computes it inline in `extract.py` (we already have `title` and `body_text` in hand), or (b) silver adds a generated column. (a) is simpler — silver stays untouched.
3. **Migration mechanism vs PRD #3/#4.** Same divergence as PRD #1 §10.1: bronze + silver use programmatic migrations under `packages/shared/src/rag_shared/migrations/*.sql`, applied at run-start. PRDs #3/#4 drafts assume `supabase/migrations/`. **Recommendation:** PRDs #3/#4 conform to programmatic migrations (`0003_*.sql`, `0004_*.sql`) so the medallion stack stays consistent.
4. **No `transformed_at` index.** Diagnostic queries by recency do a full scan. Cheap to add later if it ever matters.
5. **`primary_language` default is `'fr'`** when the URL regex doesn't match. There are no rows like that today, but the silent default is worth a unit test before a second source ships.
6. **Markdown tip-callout rewrite is HelpDocs-specific.** Adding a second source means parameterising or skipping that branch.

---

## 11. Critical files

### Implementation lives in:

- `/Users/jonas/Documents/Pracht/rag_docu_example/packages/shared/src/rag_shared/migrations/0002_silver_schema.sql`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/silver/__init__.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/silver/build.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/silver/db.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/silver/html_clean.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/silver/link_parser.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/silver/transform.py`

### Inherited from PRD #1:

- `/Users/jonas/Documents/Pracht/rag_docu_example/packages/shared/src/rag_shared/db.py` (pool + `apply_migrations`)
- `/Users/jonas/Documents/Pracht/rag_docu_example/packages/shared/src/rag_shared/env.py` (`Settings`)
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/__main__.py` (`LAYERS` dispatch)

### Config:

- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/pyproject.toml` (adds `beautifulsoup4`, `markdownify`)
