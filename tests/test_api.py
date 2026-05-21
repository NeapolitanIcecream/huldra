from __future__ import annotations

from datetime import timedelta

from fastapi.testclient import TestClient

from huldra.api import create_app
from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.keys import request_cache_key
from huldra.models import ArxivRequest, RateState
from huldra.time import utc_now
from tests.conftest import make_paper


def test_api_health_status_enqueue_and_result(settings: HuldraSettings) -> None:
    client = TestClient(create_app(settings))
    assert client.get("/healthz").json() == {"status": "ok"}
    status = client.get("/v1/status").json()
    assert status["cooldown_active"] is False

    response = client.post(
        "/v1/requests",
        json={"client_id": "api", "search_query": "cat:cs.AI", "max_results": 1},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "queued"
    result = client.get(f"/v1/results/{payload['cache_key']}").json()
    assert result["status"] == "cache_miss"


def test_api_get_result_and_paper_from_completed_cache(
    settings: HuldraSettings,
) -> None:
    store = HuldraStore(settings.db_path)
    store.init_schema()
    request = ArxivRequest(client_id="api", id_list=("2401.00001",))
    key = request_cache_key(request)
    store.record_completed_cache_entry(
        cache_key=key,
        request=request,
        papers=[make_paper("2401.00001v1")],
    )
    client = TestClient(create_app(settings))
    assert client.get(f"/v1/results/{key}").json()["papers_total"] == 1
    assert client.get("/v1/papers/2401.00001v1").json()["title"] == "Test Paper"


def test_api_serializes_cooldown_status(settings: HuldraSettings) -> None:
    store = HuldraStore(settings.db_path)
    store.init_schema()
    cooldown = utc_now() + timedelta(minutes=5)
    store.set_rate_state(RateState(cooldown_until=cooldown, last_status=429))
    client = TestClient(create_app(settings))
    payload = client.get("/v1/status").json()
    assert payload["cooldown_active"] is True
    assert payload["cooldown_until"].replace("Z", "+00:00") == cooldown.isoformat()
