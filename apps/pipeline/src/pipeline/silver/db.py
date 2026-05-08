"""Database helpers for the silver builder.

Silver does a full TRUNCATE+INSERT per run (single-source semantics per PRD #2),
so all writes are simple bulk inserts.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from psycopg import Connection

SILVER_TABLES_TRUNCATE_ORDER = (
    "silver.article_attachment_ref",
    "silver.article_translation",
    "silver.attachment",
    "silver.article",
)


def latest_run_for_source(
    conn: Connection, source: str
) -> tuple[str, str, date] | None:
    """Return (run_id, source, snapshot_date) of the latest bronze run, or None."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select run_id, source, snapshot_date
            from bronze.ingest_run
            where source = %s
            order by snapshot_date desc
            limit 1
            """,
            (source,),
        )
        return cur.fetchone()


def find_run(
    conn: Connection, source: str, snapshot_date: date
) -> tuple[str, str, date] | None:
    """Return (run_id, source, snapshot_date) for an exact (source, snapshot)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select run_id, source, snapshot_date
            from bronze.ingest_run
            where source = %s and snapshot_date = %s
            """,
            (source, snapshot_date),
        )
        return cur.fetchone()


def fetch_bronze_articles(
    conn: Connection, run_id: str
) -> list[tuple[str, dict[str, Any]]]:
    """Return [(article_id, payload), ...] for a run."""
    with conn.cursor() as cur:
        cur.execute(
            "select article_id, payload from bronze.raw_article where run_id = %s",
            (run_id,),
        )
        return cur.fetchall()


def fetch_bronze_attachments(
    conn: Connection, run_id: str
) -> list[tuple[str, str, str, int, str | None, str | None]]:
    """Return [(storage_path, kind, name, size_bytes, mimetype, etag), ...]."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select storage_path, kind, name, size_bytes, mimetype, etag
            from bronze.raw_attachment where run_id = %s
            """,
            (run_id,),
        )
        return cur.fetchall()


def truncate_silver(conn: Connection) -> None:
    sql = "truncate " + ", ".join(SILVER_TABLES_TRUNCATE_ORDER) + " cascade"
    with conn.cursor() as cur:
        cur.execute(sql)


def bulk_insert_articles(conn: Connection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = """
        insert into silver.article (
            article_id, run_id, source, snapshot_date, primary_language,
            category_id, category_path, is_published, is_private,
            helpdocs_version, canonical_url, tags
        ) values (
            %(article_id)s, %(run_id)s, %(source)s, %(snapshot_date)s, %(primary_language)s,
            %(category_id)s, %(category_path)s, %(is_published)s, %(is_private)s,
            %(helpdocs_version)s, %(canonical_url)s, %(tags)s
        )
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


def bulk_insert_translations(conn: Connection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = """
        insert into silver.article_translation (
            article_id, language_code, title, description, short_version,
            slug, url, relative_url, version_number,
            body_html, body_text, body_markdown, text_length
        ) values (
            %(article_id)s, %(language_code)s, %(title)s, %(description)s, %(short_version)s,
            %(slug)s, %(url)s, %(relative_url)s, %(version_number)s,
            %(body_html)s, %(body_text)s, %(body_markdown)s, %(text_length)s
        )
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


def bulk_insert_attachments(conn: Connection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = """
        insert into silver.attachment (
            storage_path, run_id, kind, name, article_id_from_name,
            size_bytes, mimetype, etag
        ) values (
            %(storage_path)s, %(run_id)s, %(kind)s, %(name)s, %(article_id_from_name)s,
            %(size_bytes)s, %(mimetype)s, %(etag)s
        )
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


def bulk_insert_refs(conn: Connection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = """
        insert into silver.article_attachment_ref (
            article_id, storage_path, language_code, ref_kind
        ) values (
            %(article_id)s, %(storage_path)s, %(language_code)s, %(ref_kind)s
        )
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


def count_silver(conn: Connection) -> dict[str, int]:
    counts: dict[str, int] = {}
    with conn.cursor() as cur:
        for table in (
            "silver.article",
            "silver.article_translation",
            "silver.attachment",
            "silver.article_attachment_ref",
        ):
            cur.execute(f"select count(*) from {table}")
            counts[table.split(".", 1)[1]] = int(cur.fetchone()[0])
    return counts
