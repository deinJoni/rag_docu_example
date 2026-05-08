"""CSV text extraction via stdlib csv.

When a header is detected, render each row as ``key=value; key=value`` —
preserves column semantics for embeddings without bloating tokens. Otherwise
emit comma-joined raw lines.

Module name is ``csv_extractor`` (not ``csv``) to avoid shadowing the stdlib
``csv`` module that we import here.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path

from ..models import ExtractionResult

HEURISTIC_VERSION = "v1"
EXTRACTOR_VERSION = f"csv+{HEURISTIC_VERSION}"


def _decode(raw: bytes) -> str:
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def extract(path: Path) -> ExtractionResult:
    try:
        text_in = _decode(path.read_bytes())

        try:
            sample = text_in[:8192]
            dialect = csv.Sniffer().sniff(sample)
            has_header = csv.Sniffer().has_header(sample)
        except csv.Error:
            dialect = csv.excel
            has_header = False

        reader = csv.reader(io.StringIO(text_in), dialect)
        rows = list(reader)
        if not rows:
            return ExtractionResult(
                extractor="csv",
                extractor_version=EXTRACTOR_VERSION,
                status="empty",
            )

        if has_header and len(rows) >= 2:
            header = rows[0]
            lines: list[str] = []
            for row in rows[1:]:
                pairs = [f"{h}={v}" for h, v in zip(header, row, strict=False) if v]
                if pairs:
                    lines.append("; ".join(pairs))
            text = "\n".join(lines)
        else:
            text = "\n".join(", ".join(r) for r in rows)

        return ExtractionResult(
            extractor="csv",
            extractor_version=EXTRACTOR_VERSION,
            status="ok" if text else "empty",
            text=text,
            char_count=len(text),
        )
    except Exception as err:
        return ExtractionResult(
            extractor="csv",
            extractor_version=EXTRACTOR_VERSION,
            status="error",
            error_message=str(err),
        )
