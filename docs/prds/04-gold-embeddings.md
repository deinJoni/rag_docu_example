# PRD #4 — Gold Embeddings

**Status:** Draft
**Last updated:** 2026-05-07
**Owner:** Jonas
**Depends on:** PRD #1 (Bronze ingest), PRD #2 (Silver transform — contract pinned here)
**Unblocks:** PRD #5 (Retrieval API)

## 1. Context

The repo runs a medallion-architecture RAG pipeline on Supabase. Bronze (PRD #1) lands raw articles and attachment metadata into `bronze.*` Postgres tables. Silver (PRD #2) cleans HTML and produces per-locale rows. **Gold is the embeddable layer**: it chunks silver content, calls an embedding model, and stores vectors in pgvector. The Retrieval API (PRD #5) consumes gold tables exclusively.

PRD #4 defines `gold.*` schema, the chunking algorithm, the embedding model, pgvector setup, and idempotent re-embed semantics. PDF/attachment text is explicitly deferred — it lands in the same tables later via `source_type='attachment'`.

### Corpus shape (verified against bronze storage `docs-raw/source=climkit-helpdocs/snapshot=2026-05-07/`)

- **135 articles × 4 locales (de/en/fr/it) = 540 logical documents**
- Body is HTML (`<ol><li>...</li></ol>`, occasional `<h2>`/`<h3>`)
- Per-locale plaintext: median ~125 tokens, max ~900 tokens
- ~95% of articles fit in a single chunk untouched

### Goal

Land an **idempotent gold layer** that produces ≥540 chunks with embeddings the first time it runs and 0 OpenAI API calls on every subsequent re-run when nothing upstream changed. Survive model swaps without rewriting chunk content. Survive upstream deletions cleanly.

### Locked decisions

| Decision | Choice | Rationale |
|---|---|---|
| Embedding model | **OpenAI `text-embedding-3-large`** at dim **1536** (Matryoshka truncation) | Strong multilingual quality, easy ops; corpus tiny enough that cost is irrelevant. |
| Chunking | **Whole article per chunk**; long articles (>1200 tokens) split on heading/paragraph boundaries; **no overlap**; **title prepended** to every chunk | Median article = 125 tokens — splitting fragments single thoughts. Title is ~20% of the signal at this length and must be embedded. |
| Languages | **Embed all 4 (de/en/fr/it) separately** | Native-language queries hit native-language docs first. Corpus is small; 4× cost is trivial. |
| Attachment text | **Out of scope here** | Lands later in the same `gold.chunk` via `source_type='attachment'`. PRD #4 owns the schema; PRD #3 produces the data. |

### Inherited conventions (from PRD #1)

- Migrations: **Supabase CLI** (`supabase/migrations/<timestamp>_<name>.sql`, applied with `supabase db push`)
- DB driver: **psycopg3** (already wired in `packages/shared/src/rag_shared/db.py`)
- One Postgres schema per medallion layer (`bronze`, `silver`, `gold`) — service-role only, no RLS exposure until PRD #5
- CLI: `uv run pipeline gold --source <name> --snapshot <YYYY-MM-DD>`

---

## 2. Architecture

### 2.1 Postgres schema: `gold`

Two tables — content and embedding split so re-embedding doesn't rewrite chunk content.

```sql
-- supabase/migrations/20260507100000_gold_schema.sql
create extension if not exists vector;
create schema if not exists gold;

-- one row per (article, locale, chunk_index, source_type)
create table gold.chunk (
  chunk_id        uuid primary key default gen_random_uuid(),
  article_id      text not null,
  locale          text not null check (locale in ('de','en','fr','it')),
  chunk_index     int  not null,                                     -- 0..N within (article,locale,source_type)
  chunk_text      text not null,                                     -- title prepended; embedded as-is
  token_count     int  not null,
  title           text not null,
  slug            text,
  url             text,
  category_path   text[],
  tags            text[],
  source_type     text not null default 'article'
                       check (source_type in ('article','attachment')),
  source_hash     text not null,                                     -- sha256 of upstream silver row
  created_at      timestamptz not null default now(),
  unique (article_id, locale, chunk_index, source_type)
);
create index chunk_locale_idx   on gold.chunk (locale);
create index chunk_tags_gin_idx on gold.chunk using gin (tags);

-- one row per (chunk, model, dim) — supports multi-model A/B without rewriting chunks
create table gold.chunk_embedding (
  chunk_id      uuid not null references gold.chunk(chunk_id) on delete cascade,
  model         text not null,                                       -- 'text-embedding-3-large'
  model_dim     int  not null,                                       -- 1536
  embedding     vector(1536) not null,
  content_hash  text not null,                                       -- sha256 of chunk_text
  embedded_at   timestamptz not null default now(),
  primary key (chunk_id, model, model_dim)
);
create index chunk_embedding_hnsw_idx
  on gold.chunk_embedding using hnsw (embedding vector_cosine_ops);
```

#### Rationale

- **Schema, not prefix** — `gold.chunk` matches `bronze.raw_article` / `silver.article_translation`. PostgREST can expose schemas individually; RLS scopes per-schema.
- **Two tables, not one** — re-embedding (model upgrade, dim change, provider swap) deletes/reinserts only `chunk_embedding` rows. `chunk` content stays put.
- **`content_hash` lives on `chunk_embedding`, not `chunk`** — the invariant is "this exact text was embedded by this exact model." This shape supports running two models simultaneously on the same chunk.
- **Composite PK `(chunk_id, model, model_dim)`** — same chunk can carry multiple model embeddings concurrently for A/B.
- **HNSW at 540 vectors is overkill** — sequential scan is faster at this size. Built anyway because (a) it's cheap to maintain, (b) the corpus grows once attachments and future sources land. Documented as known.
- **`locale` index alone** — retrieval filters by locale first, never by `article_id`.
- **`source_hash` on chunk** — sha256 of upstream silver `(title, body_text)`. Drives "did upstream change?" check; lets us skip re-chunking.

### 2.2 Idempotency

Two hashes drive everything:

| Layer | Hash | What it gates |
|---|---|---|
| `gold.chunk.source_hash` | sha256(`title \|\| "\n\n" \|\| body_text`) — set by silver | Skip re-chunking when upstream is unchanged |
| `gold.chunk_embedding.content_hash` | sha256(`chunk_text`) | Skip OpenAI call when `(content_hash, model, model_dim)` already present |

| Operation | Strategy |
|---|---|
| Re-chunk per `(article_id, locale, source_type)` | Transactional **DELETE then INSERT**. Cleaner than upsert when chunk count changes (N → M). Atomic so retrieval never sees a half-rebuilt article. |
| Embedding upsert | `INSERT ... ON CONFLICT (chunk_id, model, model_dim) DO UPDATE SET embedding=EXCLUDED.embedding, content_hash=EXCLUDED.content_hash, embedded_at=now()` |
| Deletion reconciliation | Any `(article_id, locale)` in `gold.chunk` but absent from silver → delete. CASCADE clears embeddings. No soft-delete at this scale. |
| Model swap | Change `EMBEDDING_MODEL` env → re-run. Old rows stay (different model in PK), new rows insert. No deletions, no chunk rewrites. |

### 2.3 Chunking algorithm

1. Build `chunk_text = f"{title}\n\n{body_text}"`. Title is high-signal at the median article length and must be in the embedded text, not just stored as a column.
2. Token-count via `tiktoken.encoding_for_model("text-embedding-3-large")` (cl100k_base).
3. If `token_count(chunk_text) ≤ 1200` → emit one chunk. (~95% of articles take this path.)
4. Else split greedily: try `<h2>` boundaries → fallback `<h3>` → fallback `\n\n` (paragraph). Min **200**, max **1200** tokens per chunk. **No overlap.** Prepend the title to every emitted sub-chunk.
5. Skip articles where `body_text` is empty after silver normalization (pure-image / video pages). Log `WARNING article=X locale=Y reason=empty_body`. Don't emit zero-token chunks.
6. Hard ceiling: cap any single sub-chunk at 1500 tokens (model max is 8191; we stay well under).

### 2.4 RLS posture

Service-role only for now. `anon` and `authenticated` roles get no grants on `gold.*`. PRD #5 decides whether retrieval is exposed via PostgREST RPC (with row-level grants) or a service-role-only Edge Function.

---

## 3. Inputs

### 3.1 Silver contract (PRD #2 must produce this)

```sql
silver.article_translation(
  article_id     text,
  locale         text,         -- 'de' | 'en' | 'fr' | 'it'
  title          text,
  body_text      text,         -- HTML stripped, plain text
  slug           text,
  url            text,
  category_path  text[],
  tags           text[],
  source_hash    text,         -- sha256 over (title || '\n\n' || body_text), set by silver
  updated_at     timestamptz,
  primary key (article_id, locale)
)
```

### 3.2 Bronze fallback (temporary)

While silver doesn't exist, gold's `extract.py` reads `bronze.raw_article.payload` directly and does HTML stripping inline using `selectolax`. The fallback is **temporary** — once PRD #2 ships, this path is deleted from gold and `selectolax` moves to a silver-only dependency.

### 3.3 Expected volumes

- **540 chunks initial** (1 per article-locale, ~95% of articles)
- **+~50 chunks** from long articles split into 2–4 sub-chunks
- **~2000 chunks total** once attachment text lands (later PRD)

---

## 4. Module layout

`+` = new file. `~` = modified file.

```
supabase/migrations/
+ 20260507100000_gold_schema.sql              (DDL from §2.1)

apps/pipeline/src/pipeline/gold/
~ __init__.py                                 (replace stub: re-export run() from load.py)
+ extract.py                                  (silver fetch + bronze fallback)
+ chunk.py                                    (token-count + heading splitter)
+ embed.py                                    (OpenAI batched calls + retry/backoff)
+ load.py                                     (orchestrator: extract → chunk → embed → upsert)
+ db.py                                       (delete-then-insert chunks; upsert embeddings)

apps/pipeline/src/pipeline/
~ __main__.py                                 (wire `pipeline gold --source --snapshot`)

apps/pipeline/
~ pyproject.toml                              (add openai>=1.50, tiktoken>=0.8, selectolax>=0.3.21)

packages/shared/src/rag_shared/
~ env.py                                      (add OpenAI + embedding settings)
~ types.py                                    (replace EmbeddedDocument stub with Chunk + ChunkEmbedding Pydantic models)
```

### 4.1 Existing utilities to reuse

- `packages/shared/src/rag_shared/env.py` → `get_settings()` already loads `.env` and is `@lru_cache`'d. Add 4 fields, keep the pattern.
- `packages/shared/src/rag_shared/db.py` (created in PRD #1) → psycopg3 connection helper. Use as-is.
- `packages/shared/src/rag_shared/supabase.py` → `get_supabase()` cached client. Used only for the bronze-fallback path.
- `apps/pipeline/src/pipeline/__main__.py` → existing `LAYERS` dispatch dict. Gold plugs in `run()` that parses `--source`/`--snapshot`.
- ThreadPoolExecutor pattern from bronze `storage.py` (8 workers, 3-attempt retry on 5xx, 0.5s × attempt backoff) — port to gold's OpenAI client.

### 4.2 Settings additions (`packages/shared/src/rag_shared/env.py`)

```python
class Settings(BaseSettings):
    # ... existing fields (supabase_url, supabase_secret_key, supabase_bucket, database_url) ...

    openai_api_key: SecretStr
    embedding_model: str = "text-embedding-3-large"
    embedding_dim: int = 1536
    gold_chunk_max_tokens: int = 1200
```

`.env` and `.env.example` get `OPENAI_API_KEY=`. The other three are optional (defaults shipped).

---

## 5. CLI contract

```
$ uv run pipeline gold --source climkit-helpdocs --snapshot 2026-05-07
[gold] reading silver.article_translation for snapshot 2026-05-07... 540 rows
[gold] chunking (max=1200 tokens)... 540 inputs → 583 chunks (43 long-article splits, 2 skipped: empty_body)
[gold] embedding 583 chunks via text-embedding-3-large@1536 (19 batches of 32)
[gold]   batch  1/19 ok (3.1s)
[gold]   ...
[gold]   batch 19/19 ok (2.7s)
[gold] cost ≈ $0.018 (137,400 tokens)
[gold] writing chunks (delete+insert per article-locale, 540 txns)
[gold] reconciling deletions... 0 stale (article,locale) tuples
[gold] done. chunks=583 embeddings=583
```

Defaults: missing `--source`/`--snapshot` → exit 1 with usage. No implicit "latest" — reproducibility matters (matches bronze).

---

## 6. Pipeline flow (`gold/load.py`)

1. Parse `--source`, `--snapshot` from argv.
2. Open psycopg connection from `rag_shared.db.get_db()`.
3. **Extract** (`extract.py`):
   - Try `SELECT ... FROM silver.article_translation` for the snapshot.
   - If silver schema doesn't exist (`pg_catalog` check), fall back: `SELECT payload FROM bronze.raw_article WHERE run_id=...`, fan out the 4 locales from `payload->'multilingual'`, HTML-strip with `selectolax`. Compute `source_hash` inline.
4. **Chunk** (`chunk.py`):
   - For each silver row, build `chunk_text`, count tokens with `tiktoken`.
   - If ≤1200 → 1 chunk. Else split (h2 → h3 → paragraph) into 200–1200-token sub-chunks. Prepend title to each.
   - Skip if `body_text` empty; log a warning. Never emit zero-token chunks.
   - Output: `list[ChunkRecord(article_id, locale, chunk_index, chunk_text, token_count, title, slug, url, category_path, tags, source_type='article', source_hash)]`.
5. **Diff against DB**: `SELECT article_id, locale, source_hash, count(*) FROM gold.chunk WHERE source_type='article' GROUP BY 1,2,3`. Skip any `(article_id, locale)` whose `source_hash` matches **and** whose chunk count matches.
6. **Embed** (`embed.py`):
   - For chunks needing new embeddings, batch 32 → `openai.embeddings.create(model='text-embedding-3-large', dimensions=1536, input=[...])`.
   - Retry on 429/5xx: 3 attempts, exponential backoff (1s, 2s, 4s).
   - Hash each chunk text → `content_hash` (sha256).
   - Skip OpenAI call if `(content_hash, model, model_dim)` already present in `gold.chunk_embedding`.
7. **Load** (`db.py`):
   - Per `(article_id, locale, source_type)`:
     ```sql
     BEGIN;
     DELETE FROM gold.chunk WHERE article_id=$1 AND locale=$2 AND source_type=$3;
     INSERT INTO gold.chunk (...) VALUES (...);
     INSERT INTO gold.chunk_embedding (...) VALUES (...) ON CONFLICT (...) DO UPDATE SET ...;
     COMMIT;
     ```
   - 540 small transactions (not one big one) — keeps locks short, partial failures recoverable.
8. **Reconcile deletions**: `DELETE FROM gold.chunk WHERE source_type='article' AND (article_id, locale) NOT IN (SELECT article_id, locale FROM silver.article_translation)`. CASCADE clears embeddings.
9. Print summary line.

### Failure semantics

- Embedding API failure after retries → log chunk_id, skip, continue. Re-run picks it up (idempotent).
- Silver query fails and no bronze fallback available → fail before any writes.
- Single article-locale transaction failure → that article rolls back; pipeline continues with the next one. Re-run replays it.

---

## 7. Cost and limits

- **Initial embed**: ~137K tokens × $0.13/M = **~$0.018** for full corpus.
- **Re-runs**: $0 (all hashes match → no API calls).
- **Model swap**: ~$0.018 again to re-embed under a new `(model, model_dim)` row.
- **OpenAI rate limits**: tier-1 = 5M TPM, far above 137K. Batch=32 keeps roundtrips ≤ 20.
- **DB write footprint**: 583 rows × 1536 dims × 4 bytes ≈ **3.6 MB** of vectors + ~1 MB chunk text.

---

## 8. Verification

End-to-end test plan, runnable after implementation:

1. **Schema applied**: `supabase db push` succeeds. SQL: `select schemaname, tablename from pg_tables where schemaname='gold';` → 2 rows. `select * from pg_extension where extname='vector';` → 1 row.
2. **Pipeline runs cold**: `uv run pipeline gold --source climkit-helpdocs --snapshot 2026-05-07` → exits 0. Summary line shows `chunks≥540 embeddings≥540`.
3. **Chunk count sanity**: `select count(*) from gold.chunk where source_type='article';` → 540–600.
4. **Embedding completeness**: `select count(*) from gold.chunk c left join gold.chunk_embedding e using (chunk_id) where e.chunk_id is null;` → 0.
5. **Locale split**: `select locale, count(*) from gold.chunk group by 1;` → 4 rows, each ~135–150.
6. **Spot-check retrieval (German)**: embed query "wie storniere ich eine Rechnung" with `text-embedding-3-large@1536` and run:
   ```sql
   select c.title, c.slug, e.embedding <=> $1::vector as distance
   from gold.chunk c
   join gold.chunk_embedding e using (chunk_id)
   where c.locale='de' and e.model='text-embedding-3-large'
   order by e.embedding <=> $1::vector
   limit 5;
   ```
   Expect "Aktionen für eine Verbraucherrechnung..." in top 3.
7. **Spot-check retrieval (Italian)**: same query intent in Italian, expect Italian results in top 5.
8. **Idempotent rerun**: rerun the CLI → 0 OpenAI API calls (all hashes match), 0 inserts. Logs show `skipped: 540/540`.
9. **Single-chunk re-embed**: `delete from gold.chunk_embedding where chunk_id = (select chunk_id from gold.chunk limit 1);`, rerun → exactly 1 OpenAI API call.
10. **Deletion reconciliation**: drop one row from `silver.article_translation` (or simulate via fallback path), rerun → corresponding chunks + embeddings disappear from gold.

---

## 9. Out of scope (explicitly deferred)

- **Attachment text → embeddings** (PRD #3 produces; this PRD's tables receive). Same `gold.chunk` + `gold.chunk_embedding`, `source_type='attachment'`. Separate ingest pass.
- **Retrieval API** (PRD #5): query-side embedding, locale routing, hybrid search (BM25 + vector), reranking.
- **RLS policies**: service-role-only for now. PRD #5 decides anon-role exposure.
- **Multi-model A/B testing**: schema supports it via composite PK; orchestration is future work.
- **Alternative chunking strategies**: proposition extraction, semantic chunking, recursive summarization. Revisit only if retrieval quality demands it.
- **Cross-encoder reranking**: PRD #5.
- **Streaming / live updates**: snapshots only.

---

## 10. Open questions for PRD #5 (retrieval)

Documented but not answered here — they're retrieval-side decisions:

- **Locale routing**: detect query language and filter `gold.chunk.locale`, or always search all locales and rerank?
- **Hybrid search**: combine BM25 (Postgres `tsvector`) with vector similarity? Where does the BM25 column live (silver or gold)?
- **Reranking**: Cohere rerank or LLM rerank for top-20 → top-5? Optional, behind a flag.
- **Cache layer**: cache query embeddings? At what scope (per-user, global, TTL)?

---

## 11. Critical files

### To create when implementing this PRD

- `supabase/migrations/20260507100000_gold_schema.sql`
- `apps/pipeline/src/pipeline/gold/extract.py`
- `apps/pipeline/src/pipeline/gold/chunk.py`
- `apps/pipeline/src/pipeline/gold/embed.py`
- `apps/pipeline/src/pipeline/gold/load.py`
- `apps/pipeline/src/pipeline/gold/db.py`

### To modify when implementing this PRD

- `apps/pipeline/src/pipeline/gold/__init__.py` (replace stub)
- `apps/pipeline/src/pipeline/__main__.py` (CLI wiring)
- `apps/pipeline/pyproject.toml` (deps)
- `packages/shared/src/rag_shared/env.py` (settings)
- `packages/shared/src/rag_shared/types.py` (Pydantic models)
- `apps/pipeline/.env`, `apps/pipeline/.env.example` (`OPENAI_API_KEY=`)
