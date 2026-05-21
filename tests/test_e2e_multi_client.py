from __future__ import annotations

from datetime import UTC, datetime, timedelta

from huldra.broker import HuldraBroker
from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.fetcher import FetchResult, RateLimitedError
from huldra.models import ArxivRequest
from huldra.worker import HuldraWorker
from tests.conftest import make_paper


class SequenceFetcher:
    def __init__(self, responses: list[object]) -> None:
        self.responses = responses
        self.calls = 0

    def fetch(self, request: ArxivRequest) -> FetchResult:
        self.calls += 1
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response  # type: ignore[return-value]


def test_two_clients_share_one_queue_item_and_one_upstream_get(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    broker = HuldraBroker(store=store, settings=settings)
    first = broker.ensure(ArxivRequest(client_id="a", search_query="cat:cs.AI"))
    second = broker.ensure(ArxivRequest(client_id="b", search_query=" cat:cs.AI ", max_results=50))
    fetcher = SequenceFetcher([FetchResult([make_paper()], total_results=1)])
    HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()
    assert first.cache_key == second.cache_key
    assert first.request_id == second.request_id
    assert fetcher.calls == 1
    assert broker.get_result(first.cache_key).papers_total == 1


def test_429_persists_cooldown_and_suppresses_second_upstream_get(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    broker = HuldraBroker(store=store, settings=settings)
    request = ArxivRequest(client_id="a", search_query="cat:cs.AI")
    first = broker.ensure(request)
    fetcher = SequenceFetcher([RateLimitedError(30)])
    HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()
    second = broker.ensure(request.model_copy(update={"client_id": "b"}))
    HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()
    assert first.cache_key == second.cache_key
    assert second.status == "cooling_down"
    assert fetcher.calls == 1
    assert store.status_summary().cooldown_active


def test_cooldown_expiry_allows_worker_to_continue(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    broker = HuldraBroker(store=store, settings=settings)
    request = ArxivRequest(client_id="a", search_query="cat:cs.AI")
    broker.ensure(request)
    fetcher = SequenceFetcher(
        [
            RateLimitedError(1),
            FetchResult([make_paper("2401.00002v1")], total_results=1),
        ]
    )
    first = HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()
    assert first.status == "rate_limited"
    store.set_rate_state(
        store.get_rate_state().model_copy(update={"cooldown_until": datetime.now(UTC) - timedelta(seconds=1)})
    )
    assert first.request_id is not None
    store.release_or_delay_queue_item(
        first.request_id,
        next_attempt_at=datetime.now(UTC) - timedelta(seconds=1),
    )
    second = HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()
    assert second.status == "completed"
    assert fetcher.calls == 2
    reopened = HuldraBroker(store=HuldraStore(settings.db_path), settings=settings)
    assert reopened.get_result(second.cache_key or "").papers_total == 1
