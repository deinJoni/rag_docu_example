"""Thin wrappers around the Supabase storage REST API for the bronze loader.

The supabase-py SDK doesn't expose retries, so we implement a small retry loop
here for transient 5xx errors during article downloads.
"""

from __future__ import annotations

import json
import time
from typing import Any

from supabase import Client


class StorageDownloadError(RuntimeError):
    """Raised when an object download fails after all retries."""


def download_bytes(
    client: Client,
    bucket: str,
    path: str,
    *,
    attempts: int = 3,
    backoff_base: float = 0.5,
) -> bytes:
    """Download an object's bytes, retrying on transient failures.

    The Supabase Python SDK raises various exception types for 5xx errors; we
    catch broadly and back off. Permanent errors (404, 403) propagate after
    the first failure since retrying won't help.
    """
    last_err: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return client.storage.from_(bucket).download(path)
        except Exception as err:
            last_err = err
            msg = str(err).lower()
            # Don't retry on definite 4xx
            if any(s in msg for s in ("404", "403", "401", "not found", "forbidden")):
                break
            if attempt < attempts:
                time.sleep(backoff_base * attempt)
    raise StorageDownloadError(f"failed to download {bucket}/{path}: {last_err}") from last_err


def download_json(
    client: Client,
    bucket: str,
    path: str,
    *,
    attempts: int = 3,
) -> dict[str, Any]:
    """Download and parse a JSON object."""
    raw = download_bytes(client, bucket, path, attempts=attempts)
    parsed = json.loads(raw.decode("utf-8"))
    if not isinstance(parsed, dict):
        raise StorageDownloadError(f"expected JSON object at {path}, got {type(parsed).__name__}")
    return parsed
