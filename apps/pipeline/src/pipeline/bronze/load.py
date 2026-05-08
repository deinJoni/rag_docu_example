"""Bronze loader: snapshot-from-storage → bronze.* tables.

Reads the ``_manifest.json`` for a snapshot from Supabase storage, upserts an
``ingest_run`` row, downloads each article JSON in parallel, upserts
``raw_article`` rows, bulk-upserts ``raw_attachment`` metadata for files and
images, and verifies counts match the manifest before declaring success.

All operations are idempotent: a re-run of the same ``(source, snapshot)``
pair refreshes rows in place. Article downloads are skipped when the existing
row's etag already matches the manifest etag.
"""

from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

from rag_shared import apply_migrations, get_pool, get_settings, get_supabase

from . import db as bronze_db
from . import storage as bronze_storage
from .manifest import Manifest, ManifestEntry, read_manifest


def _build_run_id(source: str, snapshot_date: date) -> str:
    return f"{source}:{snapshot_date.isoformat()}"


def _build_prefix(source: str, snapshot_date: date) -> str:
    return f"source={source}/snapshot={snapshot_date.isoformat()}/"


def _process_article(
    entry: ManifestEntry, run_id: str, bucket: str
) -> tuple[str, bool, str | None]:
    """Download (if needed) and upsert one article. Returns (path, did_work, error)."""
    pool = get_pool()
    sb = get_supabase()
    try:
        with pool.connection() as conn:
            existing_etag = bronze_db.get_existing_article_etag(
                conn, run_id=run_id, article_id=_extract_article_id(entry.path)
            )
            conn.commit()  # release row locks before download

        if existing_etag is not None and entry.etag and existing_etag == entry.etag:
            return entry.path, False, None  # skipped — etag match

        payload = bronze_storage.download_json(sb, bucket, entry.path)
        article_id = str(payload.get("article_id") or _extract_article_id(entry.path))

        with pool.connection() as conn:
            bronze_db.upsert_raw_article(
                conn,
                run_id=run_id,
                article_id=article_id,
                storage_path=entry.path,
                payload=payload,
                size_bytes=entry.size,
                etag=entry.etag,
                mimetype=entry.mimetype,
            )
            conn.commit()
        return entry.path, True, None
    except Exception as err:
        return entry.path, False, str(err)


def _extract_article_id(path: str) -> str:
    """Fallback article_id derivation from storage path basename ('<id>.json')."""
    name = path.rsplit("/", 1)[-1]
    return name.removesuffix(".json")


def run_bronze(source: str, snapshot_date: date, *, workers: int = 8) -> None:
    settings = get_settings()
    sb = get_supabase()
    bucket = settings.supabase_bucket
    prefix = _build_prefix(source, snapshot_date)
    run_id = _build_run_id(source, snapshot_date)

    pool = get_pool(min_size=1, max_size=workers)

    print("[bronze] applying migrations")
    with pool.connection() as conn:
        applied = apply_migrations(conn)
    if applied:
        print(f"[bronze]   applied: {', '.join(applied)}")

    print(f"[bronze] reading manifest {bucket}/{prefix}_manifest.json")
    manifest: Manifest = read_manifest(sb, bucket, prefix)

    if manifest.source != source or manifest.snapshot_date != snapshot_date:
        print(
            f"[bronze] WARNING: manifest source/date "
            f"({manifest.source}, {manifest.snapshot_date}) "
            f"does not match CLI args ({source}, {snapshot_date})",
            file=sys.stderr,
        )

    print(f"[bronze] upserting ingest_run {run_id}")
    with pool.connection() as conn:
        bronze_db.upsert_ingest_run(
            conn,
            run_id=run_id,
            source=source,
            snapshot_date=snapshot_date,
            bucket=bucket,
            prefix=prefix,
            manifest=manifest.model_dump(mode="json"),
            counts=manifest.counts.model_dump(),
        )
        conn.commit()

    by_kind = manifest.by_kind()
    articles = by_kind["article"]
    files = by_kind["file"]
    images = by_kind["image"]
    attachments = files + images

    # Articles — parallel
    print(f"[bronze] loading {len(articles)} articles ({workers} workers)...")
    t0 = time.monotonic()
    ok = 0
    skipped = 0
    failures: list[tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool_ex:
        futures = [pool_ex.submit(_process_article, e, run_id, bucket) for e in articles]
        for fut in as_completed(futures):
            path, did_work, err = fut.result()
            if err is not None:
                failures.append((path, err))
            elif did_work:
                ok += 1
            else:
                skipped += 1
    elapsed = time.monotonic() - t0
    print(
        f"[bronze] articles: ok={ok} skipped={skipped} failed={len(failures)} "
        f"in {elapsed:.1f}s"
    )
    if failures:
        for path, err in failures[:10]:
            print(f"[bronze]   FAIL {path}: {err}", file=sys.stderr)

    # Attachments — single connection, executemany
    print(f"[bronze] loading {len(attachments)} attachments (metadata only)...")
    t0 = time.monotonic()
    with pool.connection() as conn:
        n = bronze_db.bulk_upsert_attachments(conn, run_id=run_id, entries=attachments)
        conn.commit()
    elapsed = time.monotonic() - t0
    print(f"[bronze] attachments: ok={n} in {elapsed:.1f}s")

    # Verify counts before finalizing
    with pool.connection() as conn:
        actual = bronze_db.count_loaded(conn, run_id)

    expected = {
        "articles": manifest.counts.articles,
        "files": manifest.counts.files,
        "images": manifest.counts.images,
    }
    mismatches = {k: (actual[k], expected[k]) for k in expected if actual[k] != expected[k]}
    if failures or mismatches:
        if mismatches:
            print(f"[bronze] count mismatch (actual vs expected): {mismatches}", file=sys.stderr)
        print("[bronze] partial load; rerun to retry", file=sys.stderr)
        sys.exit(2)

    print(
        f"[bronze] done. articles={actual['articles']} "
        f"files={actual['files']} images={actual['images']} "
        f"total={actual['articles'] + actual['files'] + actual['images']}"
    )


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="pipeline bronze")
    p.add_argument("--source", required=True, help="e.g. climkit-helpdocs")
    p.add_argument(
        "--snapshot",
        required=True,
        type=date.fromisoformat,
        help="snapshot date YYYY-MM-DD",
    )
    p.add_argument("--workers", type=int, default=8)
    return p.parse_args(argv)


def run(argv: list[str]) -> None:
    """Entry point invoked by the pipeline CLI dispatcher.

    The pipeline ``__main__`` strips the layer name and forwards the rest of
    ``sys.argv`` to us via ``argv``.
    """
    args = parse_args(argv)
    run_bronze(args.source, args.snapshot, workers=args.workers)
