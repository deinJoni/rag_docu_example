# PRD #3 — Attachment Text Extraction

**Status:** Draft
**Last updated:** 2026-05-07
**Owner:** Jonas
**Depends on:** PRD #1 (Bronze ingest), PRD #2 (Silver transform — `silver.attachment` contract pinned here)
**Unblocks:** PRD #4 (Gold embeddings — future `source_type='attachment'` chunks consume this PRD's output)

## 1. Context

The repo runs a medallion-architecture RAG pipeline on Supabase. Bronze (PRD #1) lands raw articles + attachment metadata into `bronze.*` Postgres tables. Silver transform (PRD #2) cleans HTML, fans articles into per-locale rows (`de`, `en`, `fr`, `it`), and creates `silver.attachment` with stable surrogate IDs and dedup. Gold (PRD #4) chunks silver content and stores vectors in pgvector — its `gold.chunk.source_type` already accepts `'attachment'` as a valid value, anticipating this PRD's output.

What's missing is **attachment text**. PRD #2 catalogues the 168 attachments in `docs-raw/source=climkit-helpdocs/snapshot=2026-05-07/files/` but never opens them. Without extracted text, gold can only embed the help-article JSON. Anything a user asks about a router model, a contactor, or a wiring diagram returns nothing relevant.

This PRD adds the extraction step: for each supported attachment, download from storage, run a format-specific extractor, write rows to `silver.attachment_text`. Gold (PRD #4 follow-up) will consume those rows under `source_type='attachment'`.

### Attachment shape (verified against `docs-raw/source=climkit-helpdocs/snapshot=2026-05-07/files/`)

| Mimetype | Count | % of bytes | Notes |
|---|---:|---:|---|
| `application/pdf` | 144 | ~75 % | Technical datasheets, manuals — the corpus where most user questions get answered |
| `application/octet-stream` (`.drawio`) | 21 | small | Network diagram source XML; rendered images already exist in `images/` |
| `application/vnd.openxmlformats-officedocument.wordprocessingml.document` | 2 | small | DOCX |
| `text/csv` | 1 | tiny | One reference file |
| **total** | **168** | **~337 MB** | |

### Locked decisions (asked at plan time)

- **OCR scope: defer (flag only).** Detect image-only PDFs, mark `status='needs_ocr'`. No Tesseract / poppler / cloud-OCR dependency in this PRD. A future PRD picks up image-only handling.
- **PDF library: `pypdf` primary, `pdfplumber` fallback** on a chars-per-page heuristic (§2.3). Both MIT/BSD-licensed, pure-Python, small install.
- **`.drawio` handling: skip** — `status='unsupported'`. Rendered images for these diagrams already exist in `images/`; future image-OCR PRD covers them.

## 2. Architecture

### 2.1 Postgres schema: `silver.attachment_text`

One row per `(attachment, extractor, extractor_version)`. Lives in the `silver` schema (created by PRD #2's first migration; this PRD adds a follow-up migration).

```sql
-- supabase/migrations/20260507020000_silver_attachment_text.sql
-- (lands after PRD #2's silver schema at ...010000, before gold at ...100000)
create table silver.attachment_text (
  id                bigserial primary key,
  attachment_id     bigint      not null references silver.attachment(id) on delete cascade,
  storage_path      text        not null,                            -- denormalised for debug / log lines
  extractor         text        not null,                            -- 'pypdf' | 'pdfplumber' | 'python-docx' | 'csv'
  extractor_version text        not null,                            -- e.g. 'pypdf@5.1.0+v1' (lib version + heuristic version)
  status            text        not null check (status in
                       ('ok','empty','needs_ocr','unsupported','error')),
  text              text,                                            -- full concatenated text; null on error / unsupported
  pages             jsonb,                                           -- [{page_num, text, chars}, ...] for paginated formats
  page_count        int,                                             -- nullable for non-paginated formats
  char_count        int         not null default 0,
  error_message     text,                                            -- non-null when status='error'
  extracted_at      timestamptz not null default now(),
  unique (attachment_id, extractor, extractor_version)
);
create index attachment_text_status_idx        on silver.attachment_text (status);
create index attachment_text_attachment_id_idx on silver.attachment_text (attachment_id);
```

**Rationale:**
- Heuristic-driven fallback can yield two different extractors per attachment over time. The `unique` key includes both `extractor` and `extractor_version` so heuristic bumps create new rows alongside the old ones rather than overwriting history.
- `status` distinguishes "we tried, got nothing useful" (`needs_ocr`) from "format we don't handle" (`unsupported`) from "broke" (`error`). Each implies a different downstream action.
- Pages stored as jsonb (queryable via `->>`, `jsonb_array_elements`). If gold ever needs page-level joins for chunk-to-page citation, normalisation into `silver.attachment_text_page` is a small migration later.
- No `source_hash` here — gold's `source_hash` lives in `gold.chunk` and is computed over `(title || '\n\n' || text)`. Silver text rows don't need to redundantly hash.

### 2.2 Idempotency

| Op | Strategy |
|---|---|
| Per-attachment work selection | Anti-join: `silver.attachment` LEFT JOIN `silver.attachment_text` on `attachment_id` filtered to current `extractor_version`s — only attachments without a row at the current version get processed. |
| Row write | `INSERT ... ON CONFLICT (attachment_id, extractor, extractor_version) DO UPDATE SET status=EXCLUDED.status, text=EXCLUDED.text, pages=EXCLUDED.pages, char_count=EXCLUDED.char_count, page_count=EXCLUDED.page_count, error_message=EXCLUDED.error_message, extracted_at=now()` |
| Heuristic bump | Edit `HEURISTIC_VERSION = "v1"` → `"v2"` in `extractors/pdf.py`, rerun. New rows insert with new `extractor_version`. Old rows linger until manual `DELETE`. |
| Error retry | Rows in `status='error'` are **not** auto-retried — they only re-run when `extractor_version` changes. Documented in runner docstring. Matches PRD #1's "operator reruns explicitly" stance. |

### 2.3 Extractor strategy

| Mimetype | Primary | Fallback | Notes |
|---|---|---|---|
| `application/pdf` (144) | `pypdf` | `pdfplumber` | Chars-per-page heuristic below |
| `application/vnd.openxmlformats-officedocument.wordprocessingml.document` (2) | `python-docx` | — | Walk paragraphs + tables; pages ≈ section breaks (best-effort) |
| `text/csv` (1) | stdlib `csv` | — | Detect header → render `key=value` per row; else raw lines |
| `application/octet-stream` (the 21 `.drawio`) | — | — | One row, `status='unsupported'`, no extraction attempt |
| anything else | — | — | One row, `status='unsupported'` |

#### PDF fallback heuristic

After running pypdf:

1. `total_chars == 0` over all pages → `status='needs_ocr'`, `extractor='pypdf'`, `text=''`, `pages=[…]` with empty per-page strings.
2. Else if `mean(chars / page) < 50` → retry with pdfplumber:
   - pdfplumber gets `≥ 2.0 ×` chars → keep pdfplumber: `extractor='pdfplumber'`, `status='ok'`.
   - Else → `status='needs_ocr'`, `extractor=` whichever produced more chars, `text=` that one's output.
3. Else → `status='ok'`, `extractor='pypdf'`.

Constants in `pipeline/silver/attachment_text/extractors/pdf.py`:

```python
EMPTY_PAGE_THRESHOLD = 50      # chars/page; below → suspect image-only
FALLBACK_RATIO       = 2.0     # pdfplumber must beat pypdf by this much to win
HEURISTIC_VERSION    = "v1"    # bump to force re-extraction on next run
```

`extractor_version` written to DB combines library version and heuristic version, e.g. `f"pypdf@{importlib.metadata.version('pypdf')}+{HEURISTIC_VERSION}"`. Both library upgrades and heuristic edits are visible in row history.

## 3. Inputs

### 3.1 Upstream contract (`silver.attachment` from PRD #2)

```sql
-- contract this PRD consumes (subset; PRD #2 may add more columns):
silver.attachment(
  id            bigserial primary key,
  run_id        text       not null,        -- "{source}:{snapshot_date}"
  storage_path  text       not null,        -- 'source=climkit-helpdocs/snapshot=2026-05-07/files/<name>'
  mimetype      text       not null,        -- pulled from bronze.raw_attachment
  size_bytes    bigint     not null,
  etag          text,
  -- ... other PRD #2 fields (sha256, dedup id, etc.)
  unique (run_id, storage_path)
)
```

This PRD does **not** read from bronze directly. PRD #2 is the single source of truth for "what attachments exist for this run."

### 3.2 Storage

Binaries are pulled from Supabase Storage at `client.storage.from_(bucket).download(storage_path)`. Bucket name comes from `rag_shared.get_settings().supabase_bucket` (currently `docs-raw`).

## 4. Module layout

New files marked `+`, modified `~`. Same idiom as PRD #1's `bronze/load.py`.

```
supabase/migrations/
+ 20260507020000_silver_attachment_text.sql

apps/pipeline/src/pipeline/silver/
~ __init__.py                                 (extend run() to dispatch sub-stages: build, attachment-text)
+ attachment_text/
+   __init__.py                               (re-exports run_attachment_text)
+   runner.py                                 (orchestration: list → download → extract → upsert)
+   models.py                                 (ExtractionResult, PageText pydantic v2 models)
+   db.py                                     (silver.attachment_text upsert helper; uses get_db())
+   extractors/
+     __init__.py                             (registry: mimetype → extract callable; uniform interface)
+     pdf.py                                  (pypdf primary + pdfplumber fallback + heuristic)
+     docx.py                                 (python-docx)
+     csv.py                                  (stdlib csv → text)

apps/pipeline/
~ pyproject.toml                              (add pypdf, pdfplumber, python-docx + dev pytest)
+ tests/
+   __init__.py
+   fixtures/
+     sample-text.pdf                         (text-bearing — happy path)
+     sample-image-only.pdf                   (manufactured image-only — needs_ocr path)
+     sample.docx
+     sample.csv
+   test_extractors.py                        (per-format unit tests, no DB / no network)
+ scripts/
+   verify_attachment_text.py                 (post-run aggregate report; reuses get_db from rag_shared)
```

`apps/pipeline/src/pipeline/silver/__init__.py` becomes (sketch):

```python
def run() -> None:
    args = parse_args(sys.argv[2:])      # --source, --snapshot, --stage
    if args.stage in (None, "build"):
        run_silver_build(args)            # PRD #2
    if args.stage in (None, "attachment-text"):
        run_attachment_text(args)         # this PRD
```

### 4.1 Existing utilities to reuse (from PRD #1 / shared)

- `rag_shared.get_settings()` — Supabase URL / secret / bucket / `database_url` from env. Already loads `.env` and `apps/pipeline/.env` and is `@lru_cache`'d. No additions needed for this PRD.
- `rag_shared.get_supabase()` — storage client. Used here for `client.storage.from_(bucket).download(storage_path)` blob bytes.
- `rag_shared.get_db()` — psycopg3 connection helper introduced by PRD #1. Same `with get_db() as conn` / `with conn.cursor() as cur` idiom as `bronze/db.py`.
- ThreadPoolExecutor + 3-attempt 5xx retry pattern from `bronze/storage.py` — port the retry function.
- `LAYERS` dispatch in `__main__.py` already routes `silver` here; unchanged.

### 4.2 Extractor API contract

All format-specific extractors implement the same shape:

```python
# pipeline/silver/attachment_text/models.py
class PageText(BaseModel):
    page_num: int
    text: str
    chars: int

class ExtractionResult(BaseModel):
    extractor: str                                 # canonical id, e.g. 'pypdf'
    text: str
    pages: list[PageText] | None
    status: Literal["ok", "empty", "needs_ocr", "unsupported", "error"]
    error_message: str | None = None

# pipeline/silver/attachment_text/extractors/__init__.py
def extract(mimetype: str, path: Path) -> ExtractionResult: ...
```

The runner only consumes `ExtractionResult`; it never knows whether `pdf.py` actually fell back to `pdfplumber` inside.

## 5. CLI contract

PRD #1's pattern: `pipeline <layer> --source X --snapshot Y`. Silver follows it and adds an optional `--stage`:

```
$ uv run pipeline silver --source climkit-helpdocs --snapshot 2026-05-07
[silver] running silver-build...                                     (PRD #2)
[silver] running attachment-text extraction (4 workers)
[silver]   164 attachments to extract (4 already at extractor_version=...+v1, skipped)
[silver]   164/164 done in 73.2s
[silver]   results: ok=128 needs_ocr=14 unsupported=21 error=1
[silver] done.

$ uv run pipeline silver --source climkit-helpdocs --snapshot 2026-05-07 --stage attachment-text
# runs only this stage; same skip-already-done semantics
```

`--source` / `--snapshot` are required (no implicit "latest" — reproducibility matters; matches PRD #1).

## 6. Pipeline flow (`pipeline/silver/attachment_text/runner.py`)

1. Resolve `run_id = f"{source}:{snapshot_date}"` (PRD #1 convention).
2. At startup, compute the set of valid `extractor_version` strings (lib version + `HEURISTIC_VERSION`) for every supported extractor.
3. Build the work list with one anti-join SQL — only attachments without a row at any current `extractor_version`:
   ```sql
   select a.id, a.storage_path, a.mimetype
   from silver.attachment a
   where a.run_id = %s
     and not exists (
       select 1 from silver.attachment_text t
       where t.attachment_id = a.id
         and t.extractor_version = any(%s::text[])    -- array of supported versions
     );
   ```
4. For each work item, in a `ThreadPoolExecutor(max_workers=4)`:
   1. `client.storage.from_(bucket).download(storage_path)` (with 3-attempt 5xx retry, 0.5 s × attempt backoff — port from PRD #1).
   2. Write to `tempfile.NamedTemporaryFile(suffix=ext, delete=False)` so libraries that need a real path work.
   3. `result = extract(mimetype, Path(tmp.name))` → `ExtractionResult`.
   4. Open a short-lived psycopg connection from `get_db()`; upsert one row to `silver.attachment_text`. Single-row transaction.
   5. `os.unlink(tmp.name)` in a `finally` block.
5. After workers join: aggregate counts and print summary line.
6. Exit non-zero if `error` count > 0 (operator decides whether to chase or accept and bump version).

### Concurrency notes

- supabase-py is sync; threads suffice. If we move to httpx async later, revisit.
- Each worker takes its own DB connection (cheap with psycopg3; `get_db()` is a context manager — no shared global state).
- pypdf is pure-Python and not GIL-friendly for very large PDFs; `max_workers=4` saturates storage-pull I/O on most machines. Lift if sustained run times disappoint.

### Failure semantics

- Single-attachment extractor exception → caught, persist `status='error'` + `error_message`, continue.
- Storage download retry exhausted → `status='error'`, `error_message='download failed: <code>'`.
- DB connection failure → fail fast, no partial state (each attachment is its own transaction).
- Manifest is **not** consulted by this PRD — `silver.attachment` is the source of truth (populated by PRD #2 from bronze, which got its truth from the manifest).

## 7. Dependencies

`apps/pipeline/pyproject.toml`:

```toml
dependencies = [
  "rag-shared",
  "supabase>=2.10.0",
  "pydantic>=2.10.0",
  "pydantic-settings>=2.6.0",
+ "pypdf>=5.1.0",
+ "pdfplumber>=0.11.0",
+ "python-docx>=1.1.0",
]

[dependency-groups]
+ dev = [
+   "pytest>=8.3.0",         # introduced here for extractor unit tests
+ ]
```

Notes:
- `psycopg[binary]>=3.2` is added by PRD #1 — listed here only as a required precondition.
- No system-level dependencies (no Tesseract, no poppler).
- All three extractor libraries are MIT/BSD-style permissively licensed. No AGPL surprises.

## 8. Verification

End-to-end test plan, runnable after implementation:

1. **Migration applied:** `supabase db push` succeeds. Studio: `select tablename from pg_tables where schemaname='silver' and tablename='attachment_text';` → 1 row. Indexes present.
2. **Cold run:** `uv run pipeline silver --source climkit-helpdocs --snapshot 2026-05-07 --stage attachment-text` → exits 0 (or 1 if any `error` rows). Prints summary line.
3. **Distribution sanity:**
   ```sql
   select extractor, status, count(*), sum(char_count)
   from silver.attachment_text
   group by 1, 2
   order by 1, 2;
   ```
   Rough expectations: ~125 PDFs `ok` via pypdf, ~10 PDFs `ok` via pdfplumber fallback, ~10 PDFs `needs_ocr`, 2 docx + 1 csv `ok`, 21 drawio `unsupported`. Numbers approximate — heuristic is empirical.
4. **Sample sanity:**
   ```sql
   select substring(text, 1, 200)
   from silver.attachment_text
   where status='ok' order by random() limit 3;
   ```
   Eyeball each — should be readable text, not gibberish (no header/footer mojibake, no PDF-stream tokens leaking).
5. **Idempotent rerun:** rerun the same CLI → prints `0 to extract`, exits in <2 s.
6. **needs_ocr spot-check:** open 3 random `needs_ocr` PDFs from storage → at least 80 % should visually look image-only or scanned. If not, the heuristic is too aggressive — bump `EMPTY_PAGE_THRESHOLD` lower or `FALLBACK_RATIO` higher.
7. **Bump heuristic:** edit `HEURISTIC_VERSION = "v2"`, rerun → all rows re-extract; new rows created with new `extractor_version`. `select count(distinct extractor_version) from silver.attachment_text;` = 2.
8. **Unit tests:** `uv run pytest apps/pipeline/tests/test_extractors.py` — fixtures cover happy paths and image-only path. Pure: no DB, no storage, no network.
9. **Downstream readiness:** `select count(*) from silver.attachment_text where status='ok' and char_count > 0;` matches roughly 130 — the input gold (PRD #4) will consume under `source_type='attachment'`.

`apps/pipeline/scripts/verify_attachment_text.py` runs queries 3 + 5 + a `needs_ocr` listing + sample text snippets and prints a one-screen report.

## 9. Out of scope (explicitly deferred)

- **OCR for image-only PDFs and scanned content** → future PRD if `needs_ocr` count justifies the work. Tesseract-local vs cloud-OCR (Mistral OCR / Azure DocAI / AWS Textract) decision lives in that PRD.
- **`.drawio` XML parsing** → future PRD if rendered images don't carry enough signal for retrieval.
- **Image attachments** (the 341 in `images/`) — not text-bearing. Embeddings PRD chooses multimodal-embed or skip.
- **Chunking / token budgets / embeddings / pgvector** → gold PRD #4. This PRD's text rows are the input for gold's future `source_type='attachment'` pass.
- **Per-page row normalisation** (`silver.attachment_text_page`) — defer until gold needs it for chunk-to-page citation.
- **Multi-source extractor routing** — only `climkit-helpdocs` today; the registry is keyed by mimetype, not source.
- **Cleanup of stale `extractor_version` rows** when bumping heuristic — manual `DELETE FROM silver.attachment_text WHERE extractor_version LIKE '%+v1';` for now.
- **Cross-language detection** — extracted text language is whatever the PDF was authored in; gold's locale split for attachments is a future decision (most likely a `language_detect` heuristic at chunk time).

## 10. Open questions

1. **Migration timestamp `20260507020000`** — assumes PRD #2 chose `20260507010000`. If it chose something else (e.g. `...000001` or `...050000`), bump this PRD's filename to land after it.
2. **`pages` jsonb vs separate `silver.attachment_text_page` table** — packed jsonb today; revisit if PRD #4's chunker needs page-level joins for citation.
3. **CSV rendering convention** — `key=value` per row when header detected, else raw text. Stable for embeddings; open to changing if empirical retrieval testing prefers a different shape.
4. **`max_workers=4`** — conservative because pypdf is mildly CPU-bound. Lift to 8 if sustained run times disappoint; capacity is not a question for 168 files.
5. **Whether to fail the run on any `error` rows** — current spec exits 1. Alternative: exit 0 always, let operator inspect via `verify_attachment_text.py`. Pick the louder option (current) until we have signal that errors are acceptable noise.

## 11. Critical files

To create when implementing the extraction layer:

- `/Users/jonas/Documents/Pracht/rag_docu_example/supabase/migrations/20260507020000_silver_attachment_text.sql`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/silver/attachment_text/__init__.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/silver/attachment_text/runner.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/silver/attachment_text/models.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/silver/attachment_text/db.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/silver/attachment_text/extractors/__init__.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/silver/attachment_text/extractors/pdf.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/silver/attachment_text/extractors/docx.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/silver/attachment_text/extractors/csv.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/tests/__init__.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/tests/test_extractors.py`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/tests/fixtures/sample-text.pdf`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/tests/fixtures/sample-image-only.pdf`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/tests/fixtures/sample.docx`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/tests/fixtures/sample.csv`
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/scripts/verify_attachment_text.py`

To modify when implementing the extraction layer:

- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/src/pipeline/silver/__init__.py` (wire `--stage` parsing + `run_attachment_text()` dispatch)
- `/Users/jonas/Documents/Pracht/rag_docu_example/apps/pipeline/pyproject.toml` (3 prod deps + pytest dev dep)
