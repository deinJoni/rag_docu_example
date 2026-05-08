-- PRD #3 — silver.attachment_text
-- One row per (attachment, extractor, extractor_version). FK targets the
-- existing silver.attachment(storage_path) PK from 0002_silver_schema.sql.

CREATE TABLE IF NOT EXISTS silver.attachment_text (
    id                          bigserial PRIMARY KEY,
    attachment_storage_path     text        NOT NULL
                                    REFERENCES silver.attachment(storage_path) ON DELETE CASCADE,
    extractor                   text        NOT NULL,
    extractor_version           text        NOT NULL,
    status                      text        NOT NULL CHECK (status IN
                                    ('ok','empty','needs_ocr','unsupported','error')),
    text                        text,
    pages                       jsonb,
    page_count                  int,
    char_count                  int         NOT NULL DEFAULT 0,
    error_message               text,
    extracted_at                timestamptz NOT NULL DEFAULT now(),
    UNIQUE (attachment_storage_path, extractor, extractor_version)
);

CREATE INDEX IF NOT EXISTS attachment_text_status_idx
    ON silver.attachment_text (status);

CREATE INDEX IF NOT EXISTS attachment_text_storage_path_idx
    ON silver.attachment_text (attachment_storage_path);
