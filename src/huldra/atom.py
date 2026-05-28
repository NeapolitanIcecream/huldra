from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast
from urllib.parse import urlparse

import feedparser

from huldra.keys import arxiv_version, normalize_arxiv_id
from huldra.models import ArxivPaper


@dataclass(frozen=True, slots=True)
class ArxivAtomError:
    title: str
    message: str | None
    entry_id: str | None
    alternate_url: str | None


@dataclass(frozen=True, slots=True)
class ParsedArxivFeed:
    papers: list[ArxivPaper]
    total_results: int | None
    errors: list[ArxivAtomError]


def parse_arxiv_atom(text: str | bytes) -> ParsedArxivFeed:
    parsed = feedparser.parse(text)
    feed = cast(Any, parsed.feed)
    errors = _error_feed_entries(parsed.entries)
    if errors:
        return ParsedArxivFeed(
            papers=[],
            total_results=_coerce_int(feed.get("opensearch_totalresults")),
            errors=errors,
        )
    papers = [_paper_from_entry(entry) for entry in parsed.entries]
    return ParsedArxivFeed(
        papers=papers,
        total_results=_coerce_int(feed.get("opensearch_totalresults")),
        errors=[],
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


def _error_feed_entries(entries: list[Any]) -> list[ArxivAtomError]:
    if len(entries) != 1:
        return []
    entry = entries[0]
    title = _normalize_space(str(entry.get("title") or ""))
    if title.lower() != "error":
        return []
    raw_id = str(entry.get("id") or "").strip()
    alternate_url = _alternate_link(entry)
    if _is_paper_abs_url(raw_id):
        return []
    if not (_is_api_error_url(raw_id) or _is_api_error_url(alternate_url)):
        return []
    return [
        ArxivAtomError(
            title=title,
            message=_entry_text(entry, "summary") or _entry_text(entry, "subtitle"),
            entry_id=raw_id or None,
            alternate_url=alternate_url,
        )
    ]


def _is_paper_abs_url(value: str | None) -> bool:
    if not value:
        return False
    parsed = urlparse(str(value).strip())
    return parsed.scheme in {"http", "https"} and parsed.netloc.endswith("arxiv.org") and (
        parsed.path == "/abs" or parsed.path.startswith("/abs/")
    )


def _is_api_error_url(value: str | None) -> bool:
    if not value:
        return False
    parsed = urlparse(str(value).strip())
    path = parsed.path.rstrip("/")
    if path == "/api/errors" or path.startswith("/api/errors/"):
        return True
    fragment = parsed.fragment.strip("/")
    return fragment == "api/errors" or fragment.startswith("api/errors/")


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
