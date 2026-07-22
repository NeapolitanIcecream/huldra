from __future__ import annotations

from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import httpx
import pytest

from huldra.config import HuldraSettings
from huldra.fetcher import (
    ArxivApiFetcher,
    NonRetryableFetchError,
    RateLimitedError,
    TransientFetchError,
    _parse_retry_after_seconds,
)
from huldra.models import ArxivRequest

FEED = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>http://arxiv.org/abs/2401.00001v1</id>
    <updated>2024-01-02T00:00:00Z</updated>
    <published>2024-01-01T00:00:00Z</published>
    <title>Paper</title>
    <summary>Text.</summary>
    <author><name>Ada</name></author>
  </entry>
</feed>"""

ERROR_FEED = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>http://arxiv.org/api/errors</id>
    <link href="http://arxiv.org/api/errors" rel="alternate"/>
    <title>Error</title>
    <summary>incorrect id format</summary>
    <author><name>arXiv api core</name></author>
  </entry>
</feed>"""


def test_fetcher_turns_200_atom_into_papers(settings: HuldraSettings) -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, text=FEED)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    result = ArxivApiFetcher(settings, client=client).fetch(
        ArxivRequest(client_id="demo", search_query="cat:cs.AI")
    )
    assert [paper.arxiv_id for paper in result.papers] == ["2401.00001v1"]
    assert len(calls) == 1
    assert calls[0].headers["user-agent"] == settings.user_agent


def test_fetcher_applies_request_timeout_to_http_call(
    settings: HuldraSettings,
) -> None:
    observed_timeouts: list[dict[str, float]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        observed_timeouts.append(request.extensions["timeout"])
        return httpx.Response(200, text=FEED)

    client = httpx.Client(transport=httpx.MockTransport(handler))

    ArxivApiFetcher(settings, client=client).fetch(
        ArxivRequest(
            client_id="demo",
            search_query="cat:cs.AI",
            timeout_seconds=0.05,
        )
    )

    assert len(observed_timeouts) == 1
    assert set(observed_timeouts[0].values()) == {0.05}


def test_fetcher_rejects_200_atom_error_feed(settings: HuldraSettings) -> None:
    client = httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, text=ERROR_FEED))
    )

    with pytest.raises(NonRetryableFetchError, match="error feed"):
        ArxivApiFetcher(settings, client=client).fetch(
            ArxivRequest(client_id="demo", id_list=("not an id",))
        )


def test_fetcher_429_integer_retry_after(settings: HuldraSettings) -> None:
    client = httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(429, headers={"Retry-After": "42"}))
    )
    with pytest.raises(RateLimitedError) as exc:
        ArxivApiFetcher(settings, client=client).fetch(
            ArxivRequest(client_id="demo", search_query="cat:cs.AI")
        )
    assert exc.value.retry_after_seconds == 42
    assert exc.value.rate_limit_kind == "http_429"
    assert exc.value.api_family == "legacy_api"


def test_parse_retry_after_http_date() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    target = now + timedelta(seconds=30)
    assert _parse_retry_after_seconds(format_datetime(target), now=now) == 30


def test_parse_retry_after_http_date_never_rounds_below_the_server_boundary() -> None:
    now = datetime(2026, 1, 1, 0, 0, 0, 500_000, tzinfo=UTC)
    target = datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC)

    assert _parse_retry_after_seconds(format_datetime(target), now=now) == 30


def test_fetcher_500_and_request_error_are_transient(
    settings: HuldraSettings,
) -> None:
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI")
    with pytest.raises(TransientFetchError):
        ArxivApiFetcher(
            settings,
            client=httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(500))),
        ).fetch(request)

    def raising(_: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("offline")

    with pytest.raises(TransientFetchError):
        ArxivApiFetcher(
            settings,
            client=httpx.Client(transport=httpx.MockTransport(raising)),
        ).fetch(request)
