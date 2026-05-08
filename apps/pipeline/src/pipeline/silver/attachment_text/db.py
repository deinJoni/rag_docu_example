"""Database helpers for the attachment-text sub-stage.

- ``fetch_pending_attachments`` does the anti-join that powers idempotency:
  return only the attachments without an ``attachment_text`` row at any
  currently-supported ``extractor_version``.
- ``upsert_attachment_text`` writes one ``ExtractionResult`` to
  ``silver.attachment_text`` with ``ON CONFLICT DO UPDATE``.
- ``supported_extractor_versions`` is the canonical list of valid
  ``extractor_version`` strings used by the anti-join.
"""

from __future__ import annotations

import importlib.metadata as _md

from psycopg import Connection
from psycopg.types.json import Jsonb

from .extractors.csv_extractor import HEURISTIC_VERSION as CSV_V
from .extractors.docx import HEURISTIC_VERSION as DOCX_V
from .extractors.pdf import HEURISTIC_VERSION as PDF_V
from .models import ExtractionResult


def supported_extractor_versions() -> list[str]:
    """All extractor_version strings that should count as 'already extracted'.

    The anti-join filters attachments whose existing rows match any of these,
    so bumping any HEURISTIC_VERSION immediately surfaces those attachments
    as work on the next run.
    """
    return [
        f"pypdf@{_md.version('pypdf')}+{PDF_V}",
        f"pdfplumber@{_md.version('pdfplumber')}+{PDF_V}",
        f"python-docx@{_md.version('python-docx')}+{DOCX_V}",
        f"csv+{CSV_V}",
        "unsupported+v1",
        "download-failed+v1",  # treat past download failures as 'tried'; rerun by deleting the row
    ]


def fetch_pending_attachments(
    conn: Connection, *, run_id: str, supported_versions: list[str]
) -> list[tuple[str, str | None, str, int]]:
    """Return [(storage_path, mimetype, name, size_bytes), ...] still to extract."""
    sql = """
        select a.storage_path, a.mimetype, a.name, a.size_bytes
        from silver.attachment a
        where a.run_id = %s
          and not exists (
            select 1 from silver.attachment_text t
            where t.attachment_storage_path = a.storage_path
              and t.extractor_version = any(%s::text[])
          )
        order by a.storage_path
    """
    with conn.cursor() as cur:
        cur.execute(sql, (run_id, supported_versions))
        return cur.fetchall()


def upsert_attachment_text(
    conn: Connection, *, storage_path: str, result: ExtractionResult
) -> None:
    """Insert or refresh one row in silver.attachment_text."""
    sql = """
        insert into silver.attachment_text (
            attachment_storage_path, extractor, extractor_version, status,
            text, pages, page_count, char_count, error_message
        ) values (
            %(attachment_storage_path)s, %(extractor)s, %(extractor_version)s, %(status)s,
            %(text)s, %(pages)s, %(page_count)s, %(char_count)s, %(error_message)s
        )
        on conflict (attachment_storage_path, extractor, extractor_version) do update set
            status        = excluded.status,
            text          = excluded.text,
            pages         = excluded.pages,
            page_count    = excluded.page_count,
            char_count    = excluded.char_count,
            error_message = excluded.error_message,
            extracted_at  = now()
    """
    pages_json = (
        Jsonb([p.model_dump() for p in result.pages])
        if result.pages is not None
        else None
    )
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "attachment_storage_path": storage_path,
                "extractor": result.extractor,
                "extractor_version": result.extractor_version,
                "status": result.status,
                "text": result.text or None,
                "pages": pages_json,
                "page_count": result.page_count,
                "char_count": result.char_count,
                "error_message": result.error_message,
            },
        )


def count_attachment_text(conn: Connection) -> dict[str, int]:
    """Return per-status counts. Used by the runner's summary line."""
    counts: dict[str, int] = {}
    with conn.cursor() as cur:
        cur.execute(
            "select status, count(*) from silver.attachment_text group by 1"
        )
        for status, n in cur.fetchall():
            counts[status] = int(n)
    return counts
