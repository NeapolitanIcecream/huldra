from __future__ import annotations

import httpx
import pytest

from huldra.client import HuldraClient, HuldraHTTPError


def test_client_status_and_ensure_search_use_http_api() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/v1/status":
            return httpx.Response(
                200,
                json={
                    "upstream_requests_total": 0,
                    "upstream_429_total": 0,
                    "cooldown_active": False,
                    "queue_depth_total": 0,
                    "queue_ready_total": 0,
                    "queue_delayed_total": 0,
                    "cache_entries_total": 0,
                    "cache_completed_total": 0,
                    "cache_failed_total": 0,
                    "papers_total": 0,
                },
            )
        if request.url.path == "/v1/requests":
            return httpx.Response(
                200,
                json={
                    "status": "queued",
                    "cache_key": "huldra:v1:abc",
                    "papers_total": 0,
                },
            )
        return httpx.Response(404)

    client = HuldraClient(
        client=httpx.Client(
            base_url="http://testserver",
            transport=httpx.MockTransport(handler),
        )
    )
    assert client.status().queue_depth_total == 0
    result = client.ensure_search(search_query="cat:cs.AI", max_results=1, wait=True)
    assert result.status == "queued"
    assert requests[-1].url.params["wait"] == "true"


def test_client_raises_huldra_error_for_http_errors() -> None:
    client = HuldraClient(
        client=httpx.Client(
            base_url="http://testserver",
            transport=httpx.MockTransport(lambda request: httpx.Response(500, text="boom")),
        )
    )
    with pytest.raises(HuldraHTTPError):
        client.status()
