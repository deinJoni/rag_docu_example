"""Pydantic models for attachment-text extraction results.

The runner only ever sees ``ExtractionResult``; format-specific extractors
encode their own ``extractor`` id and ``extractor_version`` strings.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

ExtractStatus = Literal["ok", "empty", "needs_ocr", "unsupported", "error"]


class PageText(BaseModel):
    page_num: int
    text: str
    chars: int


class ExtractionResult(BaseModel):
    # extractor: 'pypdf' | 'pdfplumber' | 'python-docx' | 'csv' | 'unsupported'
    extractor: str
    extractor_version: str  # e.g. 'pypdf@5.1.0+v1'
    status: ExtractStatus
    text: str = ""
    pages: list[PageText] | None = None
    page_count: int | None = None
    char_count: int = 0
    error_message: str | None = None
