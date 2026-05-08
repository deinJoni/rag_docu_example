"""HTML body → plain text + markdown.

`body_text` is for embedding (tags collapsed, images removed, anchors flattened).
`body_markdown` is for retrieval display (preserves headings, links, images,
tip-callouts as `> NOTE:` block quotes).
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup, Comment
from markdownify import markdownify


def _strip_unwanted(soup: BeautifulSoup) -> None:
    for tag in soup(["script", "style", "iframe"]):
        tag.decompose()
    for c in soup.find_all(string=lambda t: isinstance(t, Comment)):
        c.extract()


def to_text(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    _strip_unwanted(soup)
    for img in soup.find_all("img"):
        img.decompose()
    for a in soup.find_all("a"):
        a.replace_with(a.get_text())
    raw = soup.get_text("\n")
    paragraphs: list[str] = []
    buf: list[str] = []
    for line in raw.splitlines():
        line = re.sub(r"\s+", " ", line).strip()
        if line:
            buf.append(line)
        elif buf:
            paragraphs.append(" ".join(buf))
            buf = []
    if buf:
        paragraphs.append(" ".join(buf))
    return "\n\n".join(paragraphs)


def to_markdown(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    _strip_unwanted(soup)
    # Rewrite tip-callout divs to blockquotes prefixed with NOTE.
    for div in soup.find_all("div", class_="tip-callout"):
        bq = soup.new_tag("blockquote")
        prefix = soup.new_tag("strong")
        prefix.string = "NOTE: "
        bq.append(prefix)
        for child in list(div.children):
            bq.append(child)
        div.replace_with(bq)
    md = markdownify(
        str(soup),
        heading_style="ATX",
        bullets="-",
        strip=["script", "style", "iframe"],
    )
    md = re.sub(r"\n{3,}", "\n\n", md).strip()
    return md
