"""Per-format extractor tests. Pure: no DB, no Supabase, no network."""

from __future__ import annotations

from pathlib import Path

from pipeline.silver.attachment_text.extractors import extract

from .conftest import FIXTURES_DIR


def test_pdf_text_ok() -> None:
    r = extract(
        "application/pdf", FIXTURES_DIR / "sample-text.pdf", "sample-text.pdf"
    )
    assert r.status == "ok"
    assert r.extractor == "pypdf"
    assert r.char_count > 0
    assert r.page_count == 2
    assert "Hello world" in r.text


def test_pdf_image_only_needs_ocr() -> None:
    r = extract(
        "application/pdf",
        FIXTURES_DIR / "sample-image-only.pdf",
        "sample-image-only.pdf",
    )
    assert r.status == "needs_ocr"
    # heuristic may try pdfplumber, may not — either way page_count is set.
    assert r.page_count is not None
    assert r.page_count >= 1


def test_docx_ok() -> None:
    r = extract(
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        FIXTURES_DIR / "sample.docx",
        "sample.docx",
    )
    assert r.status == "ok"
    assert r.extractor == "python-docx"
    assert "Hello from a docx fixture" in r.text
    # table cells joined with " | "
    assert "name | qty" in r.text
    assert "apple | 3" in r.text


def test_csv_with_header() -> None:
    r = extract("text/csv", FIXTURES_DIR / "sample.csv", "sample.csv")
    assert r.status == "ok"
    assert r.extractor == "csv"
    # header detected → key=value rendering
    assert "name=apple" in r.text
    assert "qty=3" in r.text


def test_unsupported_drawio(tmp_path: Path) -> None:
    """generic mimetype + .drawio extension on the canonical name → unsupported."""
    drawio = tmp_path / "diagram.drawio"
    drawio.write_text("<mxfile><diagram/></mxfile>", encoding="utf-8")
    r = extract("application/octet-stream", drawio, "diagram.drawio")
    assert r.status == "unsupported"
    assert r.extractor == "unsupported"
    assert r.char_count == 0


def test_unsupported_unknown(tmp_path: Path) -> None:
    """unknown mimetype + unknown extension → unsupported (no crash)."""
    blob = tmp_path / "thing.bin"
    blob.write_bytes(b"\x00\x01\x02")
    r = extract(None, blob, "thing.bin")
    assert r.status == "unsupported"
    assert r.extractor == "unsupported"


def test_pdf_dispatch_via_suffix_only(tmp_path: Path) -> None:
    """Even when mimetype is missing, PDF suffix routes to the pdf extractor."""
    # Re-use the text PDF fixture but pass empty mimetype.
    r = extract(
        None,
        FIXTURES_DIR / "sample-text.pdf",
        "sample-text.pdf",
    )
    assert r.extractor == "pypdf"
    assert r.status == "ok"
