CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.ingest_run (
    run_id          text PRIMARY KEY,
    source          text NOT NULL,
    snapshot_date   date NOT NULL,
    bucket          text NOT NULL,
    prefix          text NOT NULL,
    manifest        jsonb,
    counts          jsonb,
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source, snapshot_date)
);

CREATE TABLE IF NOT EXISTS bronze.raw_article (
    run_id          text NOT NULL REFERENCES bronze.ingest_run (run_id) ON DELETE CASCADE,
    article_id      text NOT NULL,
    storage_path    text NOT NULL,
    payload         jsonb NOT NULL,
    size_bytes      bigint NOT NULL,
    etag            text,
    mimetype        text,
    loaded_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, article_id)
);

CREATE TABLE IF NOT EXISTS bronze.raw_attachment (
    run_id          text NOT NULL REFERENCES bronze.ingest_run (run_id) ON DELETE CASCADE,
    storage_path    text NOT NULL,
    kind            text NOT NULL CHECK (kind IN ('file','image')),
    name            text NOT NULL,
    size_bytes      bigint NOT NULL,
    etag            text,
    mimetype        text,
    loaded_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, storage_path)
);
