from __future__ import annotations

import httpx
import pytest
from fastapi.testclient import TestClient

from huldra.api import create_app
from huldra.client import HuldraClient, HuldraHTTPError
from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.keys import request_cache_key
from huldra.models import ArxivRequest
from tests.conftest import make_paper


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


def test_client_get_paper_url_encodes_old_style_arxiv_id() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(404)

    client = HuldraClient(
        client=httpx.Client(
            base_url="http://testserver",
            transport=httpx.MockTransport(handler),
        )
    )

    assert client.get_paper("hep-th/9901001v1") is None
    assert requests[0].url.raw_path == b"/v1/papers/hep-th%2F9901001v1"


def test_client_get_paper_reads_old_style_id_from_real_app(
    settings: HuldraSettings,
) -> None:
    store = HuldraStore(settings.db_path)
    store.init_schema()
    request = ArxivRequest(client_id="api", id_list=("hep-th/9901001v1",))
    store.record_completed_cache_entry(
        cache_key=request_cache_key(request),
        request=request,
        papers=[make_paper("hep-th/9901001v1")],
    )
    test_client = TestClient(create_app(settings))

    client = HuldraClient(client=test_client)
    paper = client.get_paper("hep-th/9901001v1")

    assert paper is not None
    assert paper.arxiv_id == "hep-th/9901001v1"
