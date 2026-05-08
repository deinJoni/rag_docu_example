"""Manifest reader for the bronze layer.

Reads the ``_manifest.json`` written by the migrate-to-bronze script and parses
it into pydantic models. The manifest is the contract between the storage
snapshot and the bronze loader: it lists every file with its kind, size, etag,
and mimetype, so the loader can verify what it has.
"""

from __future__ import annotations

import json
from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, Field
from supabase import Client

ArticleKind = Literal["article"]
AttachmentKind = Literal["file", "image"]
EntryKind = Literal["article", "file", "image"]


class ManifestEntry(BaseModel):
    """A single file entry from the snapshot manifest."""

    path: str
    kind: EntryKind
    size: int
    mimetype: str | None = None
    etag: str | None = None


class ManifestCounts(BaseModel):
    articles: int = 0
    files: int = 0
    images: int = 0
    total: int = 0
    moved_ok: int | None = None
    moved_fail: int | None = None


class Manifest(BaseModel):
    """Full snapshot manifest."""

    ingest_run_id: str
    source: str
    snapshot_date: date
    ingested_at: str
    origin: dict[str, Any] = Field(default_factory=dict)
    counts: ManifestCounts
    files: list[ManifestEntry]

    def by_kind(self) -> dict[str, list[ManifestEntry]]:
        """Group entries by kind ('article', 'file', 'image')."""
        out: dict[str, list[ManifestEntry]] = {"article": [], "file": [], "image": []}
        for entry in self.files:
            out[entry.kind].append(entry)
        return out


def manifest_path(prefix: str) -> str:
    """The storage path of the manifest, given a snapshot prefix."""
    return f"{prefix.rstrip('/')}/_manifest.json"


def read_manifest(client: Client, bucket: str, prefix: str) -> Manifest:
    """Download and parse the manifest at ``<prefix>/_manifest.json``."""
    path = manifest_path(prefix)
    blob = client.storage.from_(bucket).download(path)
    data = json.loads(blob.decode("utf-8"))
    return Manifest.model_validate(data)
