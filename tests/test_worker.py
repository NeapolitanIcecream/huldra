from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.fetcher import FetchResult, RateLimitedError
from huldra.models import ArxivRequest, CachePolicy
from huldra.time import utc_now
from huldra.worker import HuldraWorker
from tests.conftest import make_paper


@dataclass
class FakeFetcher:
    responses: list[object]
    calls: int = 0

    def fetch(self, request: ArxivRequest) -> FetchResult:
        self.calls += 1
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response  # type: ignore[return-value]


def test_two_workers_cannot_claim_same_item(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    store.enqueue_request(ArxivRequest(client_id="demo", search_query="cat:cs.AI"))
    first = store.claim_next_queue_item(owner_token="w1")
    second = store.claim_next_queue_item(owner_token="w2")
    assert first is not None
    assert second is None


def test_worker_successfully_processes_queued_item(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI")
    store.enqueue_request(request)
    fetcher = FakeFetcher([FetchResult([make_paper()], total_results=1)])
    result = HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()
    assert result.status == "completed"
    assert fetcher.calls == 1
    assert store.get_cache_entry(result.cache_key or "") is not None
    assert store.status_summary().papers_total == 1


def test_worker_429_persists_cooldown_and_does_not_continue(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    store.enqueue_request(ArxivRequest(client_id="demo", search_query="cat:cs.AI"))
    store.enqueue_request(ArxivRequest(client_id="demo", search_query="cat:cs.LG"))
    fetcher = FakeFetcher([RateLimitedError(30)])
    result = HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()
    assert result.status == "rate_limited"
    assert fetcher.calls == 1
    assert store.get_rate_state().cooldown_until is not None
    assert store.status_summary().queue_depth_total == 2


def test_refresh_429_preserves_old_completed_cache(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = ArxivRequest(
        client_id="demo",
        search_query="cat:cs.AI",
        cache_policy=CachePolicy.STALE_WHILE_REVALIDATE,
    )
    from huldra.keys import request_cache_key

    key = request_cache_key(request)
    store.record_completed_cache_entry(
        cache_key=key,
        request=request,
        papers=[make_paper("2401.00001v1")],
    )
    store.enqueue_request(request, key)
    fetcher = FakeFetcher([RateLimitedError(30)])
    HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()
    entry = store.get_cache_entry(key)
    assert entry is not None
    assert entry.status == "completed"
    assert store.get_cached_papers(key)[0].arxiv_id == "2401.00001v1"


def test_worker_recovers_stale_claim(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    item = store.enqueue_request(ArxivRequest(client_id="demo", search_query="cat:cs.AI"))
    assert store.claim_next_queue_item(owner_token="w1", claim_timeout_seconds=1)
    recovered = store.claim_next_queue_item(
        owner_token="w2",
        claim_timeout_seconds=1,
        now=utc_now() + timedelta(seconds=2),
    )
    assert recovered is not None
    assert recovered.request_id == item.request_id
    assert recovered.claimed_by == "w2"
