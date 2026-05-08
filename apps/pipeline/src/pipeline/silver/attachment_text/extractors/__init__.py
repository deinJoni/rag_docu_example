"""Extractor registry: dispatch on mimetype + filename suffix.

The registry never raises — for unknown formats it returns an explicit
``status='unsupported'`` row so the runner upserts a stable record. The
``name`` argument carries the original filename (the ``path`` is a temp file
that may have a generic suffix).
"""

from __future__ import annotations

from pathlib import Path

from ..models import ExtractionResult
from . import csv_extractor, docx, pdf

PDF_MIMETYPES = {"application/pdf"}
DOCX_MIMETYPES = {
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}
CSV_MIMETYPES = {"text/csv", "application/csv"}


def extract(mimetype: str | None, path: Path, name: str) -> ExtractionResult:
    """Dispatch to a format extractor based on mimetype, falling back to the
    canonical filename's suffix when the mimetype is generic (e.g. octet-stream
    for the .drawio files in this corpus).
    """
    mt = (mimetype or "").lower()
    suffix = Path(name).suffix.lower()

    if mt in PDF_MIMETYPES or suffix == ".pdf":
        return pdf.extract(path)
    if mt in DOCX_MIMETYPES or suffix == ".docx":
        return docx.extract(path)
    if mt in CSV_MIMETYPES or suffix == ".csv":
        return csv_extractor.extract(path)

    return ExtractionResult(
        extractor="unsupported",
        extractor_version="unsupported+v1",
        status="unsupported",
        text="",
        char_count=0,
    )
