from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Any
from urllib.parse import urlparse

from huldra.models import ArxivRequest
from huldra.time import ensure_utc

NORMALIZATION_VERSION = 1
_WHITESPACE_RE = re.compile(r"\s+")
_ARXIV_PREFIX_RE = re.compile(r"^arxiv:\s*", re.IGNORECASE)
_VERSION_RE = re.compile(r"v(?P<version>\d+)$")


def normalize_search_query(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = _WHITESPACE_RE.sub(" ", value.strip())
    return normalized or None


def normalize_arxiv_id(value: str) -> str:
    raw = value.strip()
    parsed = urlparse(raw)
    if parsed.scheme and parsed.netloc:
        raw = parsed.path.lstrip("/")
    raw = _ARXIV_PREFIX_RE.sub("", raw).strip()
    raw = raw.removeprefix("abs/").removeprefix("/abs/")
    raw = raw.removeprefix("pdf/").removeprefix("/pdf/")
    raw = raw.removeprefix("e-print/").removeprefix("/e-print/")
    if raw.endswith(".pdf"):
        raw = raw[:-4]
    return raw.strip()


def arxiv_version(value: str) -> int | None:
    match = _VERSION_RE.search(value)
    if match is None:
        return None
    return int(match.group("version"))


def request_fingerprint_payload(request: ArxivRequest) -> dict[str, Any]:
    return {
        "normalization_version": NORMALIZATION_VERSION,
        "api_family": request.api_family,
        "search_query": normalize_search_query(request.search_query),
        "id_list": [normalize_arxiv_id(value) for value in request.id_list],
        "sort_by": request.sort_by,
        "sort_order": request.sort_order,
        "start": request.start,
        "max_results": request.max_results,
        "submitted_start": _iso_or_none(request.submitted_start),
        "submitted_end": _iso_or_none(request.submitted_end),
    }


def request_cache_key(request: ArxivRequest) -> str:
    payload = request_fingerprint_payload(request)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    digest = sha256(encoded).hexdigest()
    return f"huldra:v{NORMALIZATION_VERSION}:{digest}"


def build_arxiv_api_params(request: ArxivRequest) -> dict[str, str]:
    query = normalize_search_query(request.search_query)
    if request.submitted_start is not None and request.submitted_end is not None:
        start = _format_arxiv_datetime(request.submitted_start)
        inclusive_end = _format_arxiv_datetime(ensure_utc(request.submitted_end) - timedelta(seconds=1))
        date_filter = f"submittedDate:[{start} TO {inclusive_end}]"
        query = f"({query}) AND {date_filter}" if query else date_filter

    params: dict[str, str] = {
        "start": str(request.start),
        "max_results": str(request.max_results),
        "sortBy": request.sort_by,
        "sortOrder": request.sort_order,
    }
    if query:
        params["search_query"] = query
    if request.id_list:
        params["id_list"] = ",".join(normalize_arxiv_id(value) for value in request.id_list)
    return params


def _iso_or_none(value: datetime | None) -> str | None:
    if value is None:
        return None
    return ensure_utc(value).isoformat()


def _format_arxiv_datetime(value: datetime) -> str:
    value = ensure_utc(value)
    return value.strftime("%Y%m%d%H%M")
