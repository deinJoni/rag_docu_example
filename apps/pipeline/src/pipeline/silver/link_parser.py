"""Extract attachment refs and cross-article refs from a body HTML.

Outputs basenames so the orchestrator can resolve them against the loaded
`silver.attachment` set without re-walking the DOM.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from urllib.parse import unquote, urlparse

from bs4 import BeautifulSoup

CROSS_ARTICLE_RE = re.compile(r"^/l/([a-z]{2})/article/([a-z0-9]{10})-")


@dataclass
class BodyRefs:
    inline_imgs: list[str] = field(default_factory=list)
    body_links: list[str] = field(default_factory=list)
    cross_articles: list[str] = field(default_factory=list)
    external_count: int = 0


def _basename(href: str) -> str:
    path = urlparse(href).path
    return unquote(path.rsplit("/", 1)[-1])


def extract_refs(html: str, attachment_names: set[str]) -> BodyRefs:
    refs = BodyRefs()
    if not html:
        return refs
    soup = BeautifulSoup(html, "html.parser")
    for img in soup.find_all("img"):
        src = img.get("src", "") or ""
        if not src:
            continue
        name = _basename(src)
        if name in attachment_names:
            refs.inline_imgs.append(name)
    for a in soup.find_all("a"):
        href = a.get("href", "") or ""
        if not href or href.startswith(("mailto:", "tel:")):
            continue
        m = CROSS_ARTICLE_RE.match(href)
        if m:
            refs.cross_articles.append(m.group(2))
            continue
        name = _basename(href)
        if name and name in attachment_names:
            refs.body_links.append(name)
        else:
            refs.external_count += 1
    return refs
