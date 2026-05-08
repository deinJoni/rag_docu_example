"""Silver builder: bronze.raw_* → silver.* via HTML clean + link parsing.

See PRD #2. Single-source replay semantics: each run TRUNCATEs silver and
rewrites from the chosen bronze run. No external HTTP, no LLMs.
"""

from __future__ import annotations

import argparse
import time
from datetime import date
from typing import Any

from psycopg.types.json import Jsonb

from rag_shared import apply_migrations, get_pool

from . import db as silver_db
from .html_clean import to_markdown, to_text
from .link_parser import extract_refs
from .transform import parse_article_id_from_name, primary_language_from_url


def _build_article_row(
    article_id: str,
    payload: dict[str, Any],
    run_id: str,
    source: str,
    snapshot_date: date,
) -> dict[str, Any]:
    primary_lang = primary_language_from_url(payload.get("url", "")) or "fr"
    category_path = payload.get("category_path")
    tags = payload.get("tags")
    return {
        "article_id": article_id,
        "run_id": run_id,
        "source": source,
        "snapshot_date": snapshot_date,
        "primary_language": primary_lang,
        "category_id": payload.get("category_id"),
        "category_path": Jsonb(category_path) if category_path is not None else None,
        "is_published": bool(payload.get("is_published", True)),
        "is_private": bool(payload.get("is_private", False)),
        "helpdocs_version": payload.get("version_number"),
        "canonical_url": payload.get("url", ""),
        "tags": Jsonb(tags) if tags is not None else None,
    }


def _build_translation_row(article_id: str, entry: dict[str, Any]) -> dict[str, Any]:
    body_html = entry.get("body") or ""
    body_text = to_text(body_html)
    body_md = to_markdown(body_html)
    return {
        "article_id": article_id,
        "language_code": entry["language_code"],
        "title": entry.get("title", ""),
        "description": entry.get("description"),
        "short_version": entry.get("short_version"),
        "slug": entry.get("slug", ""),
        "url": entry.get("url", ""),
        "relative_url": entry.get("relative_url", ""),
        "version_number": entry.get("version_number"),
        "body_html": body_html,
        "body_text": body_text,
        "body_markdown": body_md,
        "text_length": len(body_text),
    }


def run_silver(source: str, snapshot_date: date | None = None) -> None:
    started = time.monotonic()
    pool = get_pool()

    print("[silver] applying migrations")
    with pool.connection() as conn:
        applied = apply_migrations(conn)
    if applied:
        print(f"[silver]   applied: {', '.join(applied)}")

    with pool.connection() as conn:
        if snapshot_date is None:
            row = silver_db.latest_run_for_source(conn, source)
        else:
            row = silver_db.find_run(conn, source, snapshot_date)
        if row is None:
            label = f"{source!r} {snapshot_date.isoformat() if snapshot_date else '(latest)'}"
            raise RuntimeError(
                f"[silver] no bronze.ingest_run for {label} — run `pipeline bronze` first"
            )
        run_id, source_picked, snapshot_picked = row
        print(
            f"[silver] using bronze run_id={run_id} "
            f"source={source_picked} snapshot={snapshot_picked}"
        )

        articles_raw = silver_db.fetch_bronze_articles(conn, run_id)
        attachments_raw = silver_db.fetch_bronze_attachments(conn, run_id)
        print(f"[silver]   bronze: articles={len(articles_raw)} attachments={len(attachments_raw)}")

    attachment_name_to_path = {row[2]: row[0] for row in attachments_raw}
    attachment_names = set(attachment_name_to_path.keys())

    article_rows: list[dict[str, Any]] = []
    translation_rows: list[dict[str, Any]] = []
    refs_set: set[tuple[str, str, str, str]] = set()
    counts = {"inline_img": 0, "body_link": 0, "filename_prefix": 0}
    cross_seen = 0
    external_seen = 0

    for article_id, payload in articles_raw:
        article_rows.append(
            _build_article_row(article_id, payload, run_id, source_picked, snapshot_picked)
        )
        for entry in payload.get("multilingual") or []:
            translation_rows.append(_build_translation_row(article_id, entry))
            body_html = entry.get("body") or ""
            refs = extract_refs(body_html, attachment_names)
            lang = entry["language_code"]
            for name in refs.inline_imgs:
                refs_set.add((article_id, attachment_name_to_path[name], lang, "inline_img"))
                counts["inline_img"] += 1
            for name in refs.body_links:
                refs_set.add((article_id, attachment_name_to_path[name], lang, "body_link"))
                counts["body_link"] += 1
            cross_seen += len(refs.cross_articles)
            external_seen += refs.external_count

    article_ids = {r["article_id"] for r in article_rows}
    attachment_rows: list[dict[str, Any]] = []
    for storage_path, kind, name, size_bytes, mimetype, etag in attachments_raw:
        article_id_from_name = parse_article_id_from_name(name)
        attachment_rows.append(
            {
                "storage_path": storage_path,
                "run_id": run_id,
                "kind": kind,
                "name": name,
                "article_id_from_name": article_id_from_name,
                "size_bytes": size_bytes,
                "mimetype": mimetype,
                "etag": etag,
            }
        )
        if article_id_from_name and article_id_from_name in article_ids:
            refs_set.add((article_id_from_name, storage_path, "*", "filename_prefix"))
            counts["filename_prefix"] += 1

    ref_rows = [
        {"article_id": a, "storage_path": p, "language_code": lc, "ref_kind": rk}
        for (a, p, lc, rk) in refs_set
    ]

    print(
        f"[silver] writing: articles={len(article_rows)} translations={len(translation_rows)} "
        f"attachments={len(attachment_rows)} refs={len(ref_rows)}"
    )
    with pool.connection() as conn:
        silver_db.truncate_silver(conn)
        silver_db.bulk_insert_articles(conn, article_rows)
        silver_db.bulk_insert_translations(conn, translation_rows)
        silver_db.bulk_insert_attachments(conn, attachment_rows)
        silver_db.bulk_insert_refs(conn, ref_rows)
        conn.commit()
        verify = silver_db.count_silver(conn)

    elapsed = time.monotonic() - started
    print(
        f"[silver] done. counts={verify} "
        f"refs_breakdown=(inline_img={counts['inline_img']}, body_link={counts['body_link']}, "
        f"filename_prefix={counts['filename_prefix']}) "
        f"cross_article_links_seen={cross_seen} external_links_seen={external_seen} "
        f"elapsed={elapsed:.1f}s"
    )


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="pipeline silver")
    p.add_argument("--source", default="climkit-helpdocs")
    p.add_argument(
        "--snapshot",
        type=date.fromisoformat,
        default=None,
        help="snapshot date YYYY-MM-DD (default: latest bronze run for --source)",
    )
    return p.parse_args(argv)


def run(argv: list[str]) -> None:
    args = parse_args(argv)
    run_silver(args.source, args.snapshot)
