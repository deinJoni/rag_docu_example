"""Database upsert helpers for bronze tables.

All writes are idempotent via ``ON CONFLICT ... DO UPDATE``. Each function
takes a psycopg connection (acquired by the caller from the shared pool) so
the caller controls transaction boundaries.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb

from .manifest import ManifestEntry


def upsert_ingest_run(
    conn: Connection,
    *,
    run_id: str,
    source: str,
    snapshot_date: date,
    bucket: str,
    prefix: str,
    manifest: dict[str, Any],
    counts: dict[str, Any],
) -> None:
    """Insert or refresh an ingest_run row.

    Re-running the same (source, snapshot_date) refreshes the manifest/counts
    on the existing row instead of creating a duplicate.
    """
    sql = """
        insert into bronze.ingest_run
            (run_id, source, snapshot_date, bucket, prefix, manifest, counts)
        values
            (%(run_id)s, %(source)s, %(snapshot_date)s, %(bucket)s, %(prefix)s,
             %(manifest)s, %(counts)s)
        on conflict (run_id) do update set
            source        = excluded.source,
            snapshot_date = excluded.snapshot_date,
            bucket        = excluded.bucket,
            prefix        = excluded.prefix,
            manifest      = excluded.manifest,
            counts        = excluded.counts
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "run_id": run_id,
                "source": source,
                "snapshot_date": snapshot_date,
                "bucket": bucket,
                "prefix": prefix,
                "manifest": Jsonb(manifest),
                "counts": Jsonb(counts),
            },
        )


def get_existing_article_etag(
    conn: Connection, *, run_id: str, article_id: str
) -> str | None:
    """Return the etag stored for (run_id, article_id), or None if no row."""
    with conn.cursor() as cur:
        cur.execute(
            "select etag from bronze.raw_article where run_id = %s and article_id = %s",
            (run_id, article_id),
        )
        row = cur.fetchone()
    return row[0] if row else None


def upsert_raw_article(
    conn: Connection,
    *,
    run_id: str,
    article_id: str,
    storage_path: str,
    payload: dict[str, Any],
    size_bytes: int,
    etag: str | None,
    mimetype: str | None,
) -> None:
    """Insert or refresh a raw_article row keyed by (run_id, article_id)."""
    sql = """
        insert into bronze.raw_article
            (run_id, article_id, storage_path, payload, size_bytes, etag, mimetype)
        values
            (%(run_id)s, %(article_id)s, %(storage_path)s, %(payload)s,
             %(size_bytes)s, %(etag)s, %(mimetype)s)
        on conflict (run_id, article_id) do update set
            storage_path = excluded.storage_path,
            payload      = excluded.payload,
            size_bytes   = excluded.size_bytes,
            etag         = excluded.etag,
            mimetype     = excluded.mimetype,
            loaded_at    = now()
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "run_id": run_id,
                "article_id": article_id,
                "storage_path": storage_path,
                "payload": Jsonb(payload),
                "size_bytes": size_bytes,
                "etag": etag,
                "mimetype": mimetype,
            },
        )


def bulk_upsert_attachments(
    conn: Connection, *, run_id: str, entries: list[ManifestEntry]
) -> int:
    """Bulk-upsert attachment metadata. Returns the number of rows processed.

    Does not download bytes — attachments are only metadata in bronze. The
    silver/extract layer pulls bytes from storage when needed.
    """
    if not entries:
        return 0

    sql = """
        insert into bronze.raw_attachment
            (run_id, storage_path, kind, name, size_bytes, etag, mimetype)
        values
            (%(run_id)s, %(storage_path)s, %(kind)s, %(name)s,
             %(size_bytes)s, %(etag)s, %(mimetype)s)
        on conflict (run_id, storage_path) do update set
            kind       = excluded.kind,
            name       = excluded.name,
            size_bytes = excluded.size_bytes,
            etag       = excluded.etag,
            mimetype   = excluded.mimetype,
            loaded_at  = now()
    """
    rows = [
        {
            "run_id": run_id,
            "storage_path": e.path,
            "kind": e.kind,
            "name": e.path.rsplit("/", 1)[-1],
            "size_bytes": e.size,
            "etag": e.etag,
            "mimetype": e.mimetype,
        }
        for e in entries
    ]
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


def count_loaded(conn: Connection, run_id: str) -> dict[str, int]:
    """Return row counts for a run: {'articles': n, 'files': n, 'images': n}."""
    with conn.cursor() as cur:
        cur.execute(
            "select count(*) from bronze.raw_article where run_id = %s", (run_id,)
        )
        articles = cur.fetchone()[0]  # type: ignore[index]
        cur.execute(
            """
            select kind, count(*) from bronze.raw_attachment
            where run_id = %s group by kind
            """,
            (run_id,),
        )
        per_kind = dict(cur.fetchall())
    return {
        "articles": int(articles),
        "files": int(per_kind.get("file", 0)),
        "images": int(per_kind.get("image", 0)),
    }


