from typing import Any

from pydantic import BaseModel


class RawFile(BaseModel):
    path: str
    size: int
    content_type: str | None = None
    updated_at: str


class ParsedDocument(BaseModel):
    id: str
    source: str
    content: str
    metadata: dict[str, Any]


class EmbeddedDocument(ParsedDocument):
    embedding: list[float]
