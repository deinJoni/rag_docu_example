"""Silver layer: bronze.* → silver.*

Two stages, both run by default and individually selectable via ``--stage``:

- ``build``            — HTML clean + multilingual fanout (PRD #2)
- ``attachment-text``  — extract text from PDF/DOCX/CSV attachments (PRD #3)

The CLI dispatcher in ``pipeline.__main__`` calls ``run()`` with the
layer-stripped argv tail.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

from .build import run_silver

__all__ = ["run", "run_attachment_text", "run_silver"]


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="pipeline silver")
    p.add_argument("--source", default="climkit-helpdocs")
    p.add_argument(
        "--snapshot",
        type=date.fromisoformat,
        default=None,
        help="snapshot date YYYY-MM-DD (default: latest bronze run for --source)",
    )
    p.add_argument(
        "--stage",
        choices=("build", "attachment-text"),
        default=None,
        help="run only one stage; default runs both in order",
    )
    return p.parse_args(argv)


def run(argv: list[str]) -> None:
    args = _parse_args(argv)
    exit_code = 0
    if args.stage in (None, "build"):
        run_silver(args.source, args.snapshot)
    if args.stage in (None, "attachment-text"):
        # Lazy import keeps the build-only path from pulling pypdf/pdfplumber
        # into memory and avoids a circular import (attachment_text/runner.py
        # imports pipeline.silver.db).
        from .attachment_text import run_attachment_text

        rc = run_attachment_text(args.source, args.snapshot)
        if rc != 0:
            exit_code = rc
    if exit_code != 0:
        sys.exit(exit_code)


def run_attachment_text(source: str, snapshot_date: date | None) -> int:
    """Re-export for callers that want to invoke the stage directly."""
    from .attachment_text import run_attachment_text as _impl

    return _impl(source, snapshot_date)
