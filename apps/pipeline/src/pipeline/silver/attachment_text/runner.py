"""Orchestrator for the attachment-text sub-stage.

Reads ``silver.attachment`` for a given (source, snapshot), filters out
attachments already extracted at any currently-supported ``extractor_version``,
downloads each blob from Supabase storage, runs the format-specific extractor,
and upserts a row to ``silver.attachment_text``.

Failure modes:
- A single-attachment exception inside the extractor is caught and persisted
  as ``status='error'`` so the rest of the run continues.
- A storage download failure (after retries) is persisted as ``status='error'``
  with ``error_message='download failed: ...'``.
- A migration or initial run-resolution failure raises and aborts the run.

Returns a process exit code (0 on success, 1 if any error rows were written).
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

from psycopg import Connection

from rag_shared import apply_migrations, get_pool, get_settings, get_supabase

from . import db as text_db
from .extractors import extract
from .models import ExtractionResult

WORKERS = 4


def _resolve_run(
    conn: Connection, source: str, snapshot_date: date | None
) -> tuple[str, str, date] | None:
    # Imported inline to avoid a circular import at module load: silver/__init__.py
    # pulls in this module, and silver/db.py is the parent package's submodule.
    from pipeline.silver import db as silver_db

    if snapshot_date is None:
        return silver_db.latest_run_for_source(conn, source)
    return silver_db.find_run(conn, source, snapshot_date)


def _do_one(
    storage_path: str, mimetype: str | None, name: str, bucket: str
) -> tuple[str, ExtractionResult]:
    """Worker: download blob → extract → return (storage_path, result).

    Errors are returned as ``ExtractionResult(status='error', ...)`` rather
    than raised, so a single bad attachment doesn't kill the run.
    """
    # Imported inline so an import error in pipeline.bronze doesn't crash
    # extractor-only tests that don't touch the runner.
    from pipeline.bronze.storage import download_bytes

    sb = get_supabase()
    suffix = Path(name).suffix or ".bin"
    tmp_path: Path | None = None
    try:
        try:
            blob = download_bytes(sb, bucket, storage_path)
        except Exception as err:
            return storage_path, ExtractionResult(
                extractor="unsupported",
                extractor_version="download-failed+v1",
                status="error",
                error_message=f"download failed: {err}",
            )

        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp.write(blob)
            tmp_path = Path(tmp.name)

        return storage_path, extract(mimetype, tmp_path, name)
    finally:
        if tmp_path is not None:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


def run_attachment_text(source: str, snapshot_date: date | None) -> int:
    """Run the attachment-text extraction pass. Returns exit code."""
    started = time.monotonic()
    settings = get_settings()
    bucket = settings.supabase_bucket
    pool = get_pool(min_size=1, max_size=WORKERS + 1)

    print("[silver:attachment-text] applying migrations")
    with pool.connection() as conn:
        applied = apply_migrations(conn)
    if applied:
        print(f"[silver:attachment-text]   applied: {', '.join(applied)}")

    with pool.connection() as conn:
        row = _resolve_run(conn, source, snapshot_date)
        if row is None:
            label = (
                f"{source!r} "
                f"{snapshot_date.isoformat() if snapshot_date else '(latest)'}"
            )
            raise RuntimeError(
                f"[silver:attachment-text] no bronze.ingest_run for {label} — "
                "run `pipeline silver` (build stage) first"
            )
        run_id, source_picked, snapshot_picked = row
        print(
            f"[silver:attachment-text] using run_id={run_id} "
            f"source={source_picked} snapshot={snapshot_picked}"
        )

        versions = text_db.supported_extractor_versions()
        pending = text_db.fetch_pending_attachments(
            conn, run_id=run_id, supported_versions=versions
        )

    if not pending:
        elapsed = time.monotonic() - started
        print(
            f"[silver:attachment-text] nothing to extract (all up-to-date) "
            f"in {elapsed:.1f}s"
        )
        return 0

    print(
        f"[silver:attachment-text] {len(pending)} attachments to extract "
        f"({WORKERS} workers)"
    )

    counts: Counter[str] = Counter()
    errors: list[tuple[str, str]] = []

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {
            ex.submit(_do_one, sp, mt, name, bucket): sp
            for (sp, mt, name, _size) in pending
        }
        for fut in as_completed(futures):
            storage_path, result = fut.result()
            counts[result.status] += 1
            if result.status == "error":
                errors.append((storage_path, result.error_message or "unknown"))
            with pool.connection() as conn:
                text_db.upsert_attachment_text(
                    conn, storage_path=storage_path, result=result
                )
                conn.commit()

    elapsed = time.monotonic() - started
    summary = " ".join(f"{k}={v}" for k, v in sorted(counts.items()))
    print(f"[silver:attachment-text] done in {elapsed:.1f}s  results: {summary}")

    if errors:
        print(
            f"[silver:attachment-text] {len(errors)} errors (showing up to 10):",
            file=sys.stderr,
        )
        for sp, msg in errors[:10]:
            print(f"  FAIL {sp}: {msg}", file=sys.stderr)
        return 1

    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Standalone argparse for direct invocation (the silver dispatcher
    has its own parser; this is here for ad-hoc CLI use)."""
    p = argparse.ArgumentParser(prog="pipeline silver attachment-text")
    p.add_argument("--source", default="climkit-helpdocs")
    p.add_argument("--snapshot", type=date.fromisoformat, default=None)
    return p.parse_args(argv)


def run(argv: list[str]) -> None:
    args = parse_args(argv)
    rc = run_attachment_text(args.source, args.snapshot)
    if rc != 0:
        sys.exit(rc)
