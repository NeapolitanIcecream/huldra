from __future__ import annotations

from datetime import UTC, datetime, timedelta

from huldra.db import HuldraStore
from huldra.keys import request_cache_key
from huldra.models import ArxivRequest, RateState, RequestStatus
from huldra.time import utc_now
from tests.conftest import make_paper


def test_store_records_completed_cache_and_reads_ordered_papers(
    store: HuldraStore,
) -> None:
    request = ArxivRequest(client_id="demo", id_list=("2401.00001",))
    key = request_cache_key(request)
    store.record_completed_cache_entry(
        cache_key=key,
        request=request,
        papers=[make_paper("2401.00001v1")],
        total_results=1,
    )
    entry = store.get_cache_entry(key)
    papers = store.get_cached_papers(key)
    assert entry is not None
    assert entry.status == "completed"
    assert entry.total_results == 1
    assert [paper.arxiv_id for paper in papers] == ["2401.00001v1"]


def test_enqueue_dedupes_pending_cache_key(store: HuldraStore) -> None:
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI")
    first = store.enqueue_request(request)
    second = store.enqueue_request(request)
    assert second.request_id == first.request_id


def test_claim_next_queue_item_is_exclusive_until_claim_expires(
    store: HuldraStore,
) -> None:
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI")
    item = store.enqueue_request(request)
    first = store.claim_next_queue_item(owner_token="w1", claim_timeout_seconds=60)
    second = store.claim_next_queue_item(owner_token="w2", claim_timeout_seconds=60)
    assert first is not None
    assert first.request_id == item.request_id
    assert second is None
    stale_time = utc_now() + timedelta(seconds=61)
    recovered = store.claim_next_queue_item(
        owner_token="w2",
        claim_timeout_seconds=60,
        now=stale_time,
    )
    assert recovered is not None
    assert recovered.request_id == item.request_id
    assert recovered.claimed_by == "w2"


def test_rate_state_and_leases_are_durable(store: HuldraStore) -> None:
    cooldown = datetime(2026, 1, 1, tzinfo=UTC)
    store.set_rate_state(
        RateState(
            name="arxiv_legacy_api",
            cooldown_until=cooldown,
            consecutive_429_total=2,
            last_status=429,
        )
    )
    assert store.get_rate_state().cooldown_until == cooldown
    assert store.acquire_lease("upstream_fetch", "w1", 60)
    assert not store.acquire_lease("upstream_fetch", "w2", 60)
    store.release_lease("upstream_fetch", "w1")
    assert store.acquire_lease("upstream_fetch", "w2", 60)


def test_release_or_delay_marks_queue_item_failed(store: HuldraStore) -> None:
    item = store.enqueue_request(ArxivRequest(client_id="demo", search_query="cat:cs.AI"))
    store.release_or_delay_queue_item(
        item.request_id,
        status=RequestStatus.FAILED,
        error_category="non_retryable",
        error_message="bad request",
    )
    updated = store.get_queue_item(item.request_id)
    assert updated is not None
    assert updated.status == RequestStatus.FAILED
    assert updated.error_category == "non_retryable"
