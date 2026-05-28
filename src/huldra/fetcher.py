from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from huldra.atom import parse_arxiv_atom
from huldra.config import HuldraSettings
from huldra.keys import build_arxiv_api_params
from huldra.models import ArxivPaper, ArxivRequest
from huldra.time import ensure_utc, utc_now


class HuldraFetchError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class RateLimitedError(HuldraFetchError):
    def __init__(self, retry_after_seconds: int | None, message: str = "rate limited") -> None:
        super().__init__(message, status_code=429)
        self.retry_after_seconds = retry_after_seconds


class TransientFetchError(HuldraFetchError):
    pass


class NonRetryableFetchError(HuldraFetchError):
    pass


@dataclass(frozen=True, slots=True)
class FetchResult:
    papers: list[ArxivPaper]
    total_results: int | None
    upstream_status: int = 200


class ArxivApiFetcher:
    def __init__(
        self,
        settings: HuldraSettings,
        *,
        client: httpx.Client | None = None,
    ) -> None:
        self.settings = settings
        self._client = client

    def fetch(self, request: ArxivRequest) -> FetchResult:
        params = build_arxiv_api_params(request)
        headers = {"User-Agent": self.settings.user_agent}
        if self._client is None:
            with httpx.Client(timeout=self.settings.request_timeout_seconds) as client:
                response = self._get_once(client, params, headers)
        else:
            response = self._get_once(self._client, params, headers)

        if response.status_code == 429:
            raise RateLimitedError(_parse_retry_after_seconds(response.headers.get("Retry-After")))
        if response.status_code >= 500:
            raise TransientFetchError(
                f"arXiv API returned HTTP {response.status_code}",
                status_code=response.status_code,
            )
        if response.status_code < 200 or response.status_code >= 300:
            raise NonRetryableFetchError(
                f"arXiv API returned HTTP {response.status_code}",
                status_code=response.status_code,
            )
        parsed = parse_arxiv_atom(response.text)
        if parsed.errors:
            message = parsed.errors[0].message or parsed.errors[0].title
            raise NonRetryableFetchError(
                f"arXiv API returned an error feed: {message}",
                status_code=response.status_code,
            )
        return FetchResult(
            papers=parsed.papers,
            total_results=parsed.total_results,
            upstream_status=response.status_code,
        )

    def _get_once(
        self,
        client: httpx.Client,
        params: dict[str, str],
        headers: dict[str, str],
    ) -> httpx.Response:
        try:
            return client.get(self.settings.arxiv_api_url, params=params, headers=headers)
        except httpx.RequestError as exc:
            raise TransientFetchError(f"arXiv API request failed: {exc}") from exc


def _parse_retry_after_seconds(raw: Any, *, now: datetime | None = None) -> int | None:
    if raw is None:
        return None
    value = str(raw).strip()
    if not value:
        return None
    if value.isdigit():
        return max(0, int(value))
    try:
        target = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if target.tzinfo is None:
        target = target.replace(tzinfo=UTC)
    delta = ensure_utc(target) - ensure_utc(now or utc_now())
    return max(0, int(delta.total_seconds()))
