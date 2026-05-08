"""PDF text extraction with a pypdf primary / pdfplumber fallback heuristic.

Heuristic (per PRD #3):
  1. Run pypdf. If total chars across pages == 0 → image-only PDF, return
     ``status='needs_ocr'``.
  2. Else if mean(chars/page) < EMPTY_PAGE_THRESHOLD → retry with pdfplumber:
     - if pdfplumber gets >= FALLBACK_RATIO * pypdf's char count → keep
       pdfplumber, ``status='ok'``.
     - else → ``status='needs_ocr'``, keep whichever produced more chars.
  3. Else → pypdf, ``status='ok'``.

Bumping HEURISTIC_VERSION forces a re-extract on next run (the
``extractor_version`` string changes, so the anti-join in the runner picks up
every PDF again).
"""

from __future__ import annotations

import importlib.metadata
from pathlib import Path

from ..models import ExtractionResult, ExtractStatus, PageText

EMPTY_PAGE_THRESHOLD = 50    # chars/page; below → suspect image-only / column-broken
FALLBACK_RATIO = 2.0         # pdfplumber must beat pypdf by this much to win
HEURISTIC_VERSION = "v1"


def _pypdf_pages(path: Path) -> list[PageText]:
    import pypdf

    reader = pypdf.PdfReader(str(path))
    out: list[PageText] = []
    for i, page in enumerate(reader.pages):
        try:
            text = page.extract_text() or ""
        except Exception:  # pypdf can throw on malformed pages — keep going
            text = ""
        out.append(PageText(page_num=i + 1, text=text, chars=len(text)))
    return out


def _pdfplumber_pages(path: Path) -> list[PageText]:
    import pdfplumber

    out: list[PageText] = []
    with pdfplumber.open(str(path)) as pdf_doc:
        for i, page in enumerate(pdf_doc.pages):
            try:
                text = page.extract_text() or ""
            except Exception:
                text = ""
            out.append(PageText(page_num=i + 1, text=text, chars=len(text)))
    return out


def _version(lib: str) -> str:
    return f"{lib}@{importlib.metadata.version(lib)}+{HEURISTIC_VERSION}"


def _result_from(
    pages: list[PageText], extractor: str, status: ExtractStatus
) -> ExtractionResult:
    text = "\n\n".join(p.text for p in pages if p.text)
    return ExtractionResult(
        extractor=extractor,
        extractor_version=_version(extractor),
        status=status,
        text=text,
        pages=pages,
        page_count=len(pages),
        char_count=sum(p.chars for p in pages),
    )


def extract(path: Path) -> ExtractionResult:
    try:
        py_pages = _pypdf_pages(path)
    except Exception as err:
        return ExtractionResult(
            extractor="pypdf",
            extractor_version=_version("pypdf"),
            status="error",
            error_message=str(err),
        )

    py_chars = sum(p.chars for p in py_pages)

    # Case 1: zero text → almost certainly image-only PDF; skip fallback.
    if py_chars == 0:
        return _result_from(py_pages, "pypdf", "needs_ocr")

    # Case 2: thin text → try pdfplumber.
    avg_chars_per_page = py_chars / max(len(py_pages), 1)
    if avg_chars_per_page < EMPTY_PAGE_THRESHOLD:
        try:
            pl_pages = _pdfplumber_pages(path)
        except Exception:
            return _result_from(py_pages, "pypdf", "needs_ocr")

        pl_chars = sum(p.chars for p in pl_pages)
        if pl_chars >= FALLBACK_RATIO * py_chars and pl_chars > 0:
            return _result_from(pl_pages, "pdfplumber", "ok")

        # Fallback didn't help meaningfully — flag for OCR, keep richer output.
        if pl_chars > py_chars:
            return _result_from(pl_pages, "pdfplumber", "needs_ocr")
        return _result_from(py_pages, "pypdf", "needs_ocr")

    # Case 3: pypdf produced healthy text.
    return _result_from(py_pages, "pypdf", "ok")
