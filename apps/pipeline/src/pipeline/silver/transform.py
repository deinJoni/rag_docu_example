"""Pure-function transforms used by the silver orchestrator."""

from __future__ import annotations

import re

LEADING_ID_RE = re.compile(r"^([a-z0-9]{10})-\d+-")
URL_LANG_RE = re.compile(r"/l/([a-z]{2})/")


def parse_article_id_from_name(name: str) -> str | None:
    m = LEADING_ID_RE.match(name)
    return m.group(1) if m else None


def primary_language_from_url(url: str) -> str | None:
    m = URL_LANG_RE.search(url or "")
    return m.group(1) if m else None
