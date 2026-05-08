CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.article (
    article_id          text PRIMARY KEY,
    run_id              text NOT NULL REFERENCES bronze.ingest_run (run_id),
    source              text NOT NULL,
    snapshot_date       date NOT NULL,
    primary_language    text NOT NULL,
    category_id         text,
    category_path       jsonb,
    is_published        boolean NOT NULL,
    is_private          boolean NOT NULL,
    helpdocs_version    int,
    canonical_url       text NOT NULL,
    tags                jsonb,
    transformed_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.article_translation (
    article_id          text NOT NULL REFERENCES silver.article ON DELETE CASCADE,
    language_code       text NOT NULL,
    title               text NOT NULL,
    description         text,
    short_version       text,
    slug                text NOT NULL,
    url                 text NOT NULL,
    relative_url        text NOT NULL,
    version_number      int,
    body_html           text NOT NULL,
    body_text           text NOT NULL,
    body_markdown       text NOT NULL,
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
    article_id_from_name    text,
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
    language_code   text NOT NULL,
    ref_kind        text NOT NULL CHECK (ref_kind IN ('inline_img','body_link','filename_prefix')),
    PRIMARY KEY (article_id, storage_path, language_code, ref_kind)
);
