from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

import feedparser

from huldra.keys import arxiv_version, normalize_arxiv_id
from huldra.models import ArxivPaper


@dataclass(frozen=True, slots=True)
class ParsedArxivFeed:
    papers: list[ArxivPaper]
    total_results: int | None


def parse_arxiv_atom(text: str | bytes) -> ParsedArxivFeed:
    parsed = feedparser.parse(text)
    papers = [_paper_from_entry(entry) for entry in parsed.entries]
    feed = cast(Any, parsed.feed)
    return ParsedArxivFeed(
        papers=papers,
        total_results=_coerce_int(feed.get("opensearch_totalresults")),
    )


def _paper_from_entry(entry: Any) -> ArxivPaper:
    raw_id = str(entry.get("id") or _alternate_link(entry) or "")
    arxiv_id = normalize_arxiv_id(raw_id)
    version = arxiv_version(arxiv_id)
    authors = [
        str(author.get("name")).strip()
        for author in entry.get("authors", [])
        if str(author.get("name") or "").strip()
    ]
    categories = [
        str(tag.get("term")).strip() for tag in entry.get("tags", []) if str(tag.get("term") or "").strip()
    ]
    primary_category = _primary_category(entry) or (categories[0] if categories else None)
    canonical_url = _alternate_link(entry) or f"https://arxiv.org/abs/{arxiv_id}"
    return ArxivPaper(
        arxiv_id=arxiv_id,
        version=version,
        canonical_url=canonical_url.replace("http://", "https://"),
        title=_normalize_space(str(entry.get("title") or "")),
        abstract=_normalize_space(str(entry.get("summary") or "")) or None,
        authors=authors,
        primary_category=primary_category,
        categories=categories,
        published_at=_entry_datetime(entry, "published"),
        updated_at=_entry_datetime(entry, "updated"),
        comment=_entry_text(entry, "arxiv_comment"),
        journal_ref=_entry_text(entry, "arxiv_journal_ref"),
        doi=_entry_text(entry, "arxiv_doi"),
        raw_atom={
            "entry_id": raw_id,
            "alternate_url": canonical_url,
            "pdf_url": _pdf_link(entry),
        },
    )


def _normalize_space(value: str) -> str:
    return " ".join(value.split())


def _entry_text(entry: Any, key: str) -> str | None:
    value = entry.get(key)
    if value is None:
        return None
    normalized = _normalize_space(str(value))
    return normalized or None


def _primary_category(entry: Any) -> str | None:
    raw = entry.get("arxiv_primary_category")
    if isinstance(raw, dict):
        term = raw.get("term")
        return str(term).strip() if term else None
    return None


def _alternate_link(entry: Any) -> str | None:
    for link in entry.get("links", []):
        if link.get("rel") == "alternate":
            href = str(link.get("href") or "").strip()
            if href:
                return href
    return None


def _pdf_link(entry: Any) -> str | None:
    for link in entry.get("links", []):
        href = str(link.get("href") or "").strip()
        title = str(link.get("title") or "").lower()
        content_type = str(link.get("type") or "").lower()
        if href and (title == "pdf" or content_type == "application/pdf"):
            return href
    return None


def _entry_datetime(entry: Any, field: str) -> datetime | None:
    parsed = entry.get(f"{field}_parsed")
    if parsed:
        return datetime(*parsed[:6], tzinfo=UTC)
    value = entry.get(field)
    if not value:
        return None
    raw = str(value).replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
