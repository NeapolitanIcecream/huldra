from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from huldra.api import create_app
from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.keys import request_cache_key
from huldra.models import ArxivRequest, OaiRecord, RateState
from huldra.oai import OaiPmhPage
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
    result = client.get(f"/v1/results/{key}").json()
    assert result["serving_mode"] == "raw_inspection"
    assert result["papers_total"] == 1
    assert "analysis_ready" not in result
    assert client.get("/v1/papers/2401.00001v1").json()["title"] == "Test Paper"


def test_api_raw_inspection_reports_failures_without_readiness_blocked_reason(
    settings: HuldraSettings,
) -> None:
    store = HuldraStore(settings.db_path)
    store.init_schema()
    request = ArxivRequest(client_id="api", search_query="cat:cs.AI")
    key = request_cache_key(request)
    store.record_cache_failure(
        cache_key=key,
        request=request,
        error_category="non_retryable",
        error_message="bad request",
        upstream_status=400,
    )

    result = TestClient(create_app(settings)).get(f"/v1/results/{key}").json()

    assert result["serving_mode"] == "raw_inspection"
    assert result["status"] == "failed"
    assert result["blocked_reason"] is None
    assert result["error_category"] == "non_retryable"


def test_api_get_paper_supports_old_style_arxiv_id_with_slash(
    settings: HuldraSettings,
) -> None:
    store = HuldraStore(settings.db_path)
    store.init_schema()
    paper = make_paper("hep-th/9901001v1")
    request = ArxivRequest(client_id="api", id_list=("hep-th/9901001v1",))
    store.record_completed_cache_entry(
        cache_key=request_cache_key(request),
        request=request,
        papers=[paper],
    )

    client = TestClient(create_app(settings))

    assert client.get("/v1/papers/hep-th/9901001v1").status_code == 200
    encoded = client.get("/v1/papers/hep-th%2F9901001v1")
    assert encoded.status_code == 200
    assert encoded.json()["arxiv_id"] == "hep-th/9901001v1"


def test_api_get_paper_serves_versioned_alias_from_oai_base_row(
    settings: HuldraSettings,
) -> None:
    store = HuldraStore(settings.db_path)
    store.init_schema()
    datestamp = datetime(2026, 5, 28, tzinfo=UTC)
    oai_paper = make_paper("2401.00008").model_copy(
        update={
            "version": None,
            "title": "OAI Base",
            "oai_identifier": "oai:arXiv.org:2401.00008",
            "oai_datestamp": datestamp,
            "oai_set_specs": ["cs:cs:AI"],
        }
    )
    store.upsert_oai_records(
        [
            OaiRecord(
                oai_identifier="oai:arXiv.org:2401.00008",
                arxiv_id="2401.00008",
                metadata_prefix="arXiv",
                datestamp=datestamp,
                set_specs=["cs:cs:AI"],
                paper=oai_paper,
            )
        ]
    )

    response = TestClient(create_app(settings)).get("/v1/papers/2401.00008v1")

    assert response.status_code == 200
    assert response.json()["arxiv_id"] == "2401.00008"
    assert response.json()["title"] == "OAI Base"


def test_api_serializes_cooldown_status(settings: HuldraSettings) -> None:
    store = HuldraStore(settings.db_path)
    store.init_schema()
    cooldown = utc_now() + timedelta(minutes=5)
    store.set_rate_state(RateState(cooldown_until=cooldown, last_status=429))
    client = TestClient(create_app(settings))
    payload = client.get("/v1/status").json()
    assert payload["cooldown_active"] is True
    assert payload["cooldown_until"].replace("Z", "+00:00") == cooldown.isoformat()


def test_api_sync_endpoint_returns_maintenance_summary(settings: HuldraSettings) -> None:
    client = TestClient(create_app(settings))

    response = client.post(
        "/v1/sync",
        json={"requests": [{"client_id": "api", "search_query": "cat:cs.AI"}]},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["requested_total"] == 1
    assert payload["queued_total"] == 1
    assert payload["requests"][0]["raw_cache_status"] == "queued"


def test_api_sync_rejects_complete_window_without_wait(settings: HuldraSettings) -> None:
    response = TestClient(create_app(settings)).post(
        "/v1/sync",
        json={
            "requests": [{"client_id": "api", "search_query": "cat:cs.AI"}],
            "mode": "complete_window",
            "wait": False,
        },
    )

    assert response.status_code == 422
    assert "requires wait=True" in response.text


def test_api_legacy_request_surfaces_reject_oai_family(settings: HuldraSettings) -> None:
    client = TestClient(create_app(settings))

    request_response = client.post(
        "/v1/requests",
        json={
            "client_id": "api",
            "search_query": "cat:cs.AI",
            "api_family": "oai_pmh",
        },
    )
    sync_response = client.post(
        "/v1/sync",
        json={
            "requests": [
                {
                    "client_id": "api",
                    "search_query": "cat:cs.AI",
                    "api_family": "oai_pmh",
                }
            ]
        },
    )

    assert request_response.status_code == 422
    assert sync_response.status_code == 422
    assert "OaiHarvestRequest" in request_response.text
    assert "OaiHarvestRequest" in sync_response.text


def test_api_backfill_endpoint_plans_windows(settings: HuldraSettings) -> None:
    client = TestClient(create_app(settings))

    response = client.post(
        "/v1/backfill",
        json={
            "search_queries": ["cat:cs.AI"],
            "start_date": "2026-01-01",
            "end_date": "2026-01-02",
            "max_results": 10,
            "wait": False,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["requested_total"] == 2
    assert payload["queued_total"] == 2


def test_api_harvest_oai_endpoint_runs_harvest(
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    class FakeOaiPmhFetcher:
        def __init__(self, settings: HuldraSettings) -> None:
            self.settings = settings

        def list_records(self, **kwargs: object) -> OaiPmhPage:
            return OaiPmhPage(
                records=[],
                response_date="2026-05-28T00:00:00Z",
                resumption_token=None,
                errors=[],
                request_params={"verb": "ListRecords", "metadataPrefix": "arXiv"},
            )

    monkeypatch.setattr("huldra.broker.OaiPmhFetcher", FakeOaiPmhFetcher)
    client = TestClient(create_app(settings))

    response = client.post(
        "/v1/harvest/oai",
        json={"client_id": "api", "metadata_prefix": "arXiv", "mode": "initial"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "completed"
    assert payload["pages_total"] == 1
    assert payload["current_watermark"] == "2026-05-28"
