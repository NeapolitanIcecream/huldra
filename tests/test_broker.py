from __future__ import annotations

from datetime import timedelta

from huldra.broker import HuldraBroker
from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.keys import request_cache_key
from huldra.models import ArxivRequest, CachePolicy, RateState
from huldra.time import utc_now
from tests.conftest import make_paper


def test_broker_returns_completed_cache_hit(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI")
    key = request_cache_key(request)
    store.record_completed_cache_entry(
        cache_key=key,
        request=request,
        papers=[make_paper()],
    )
    result = HuldraBroker(store=store, settings=settings).ensure(request)
    assert result.status == "ready"
    assert result.cache_hit
    assert result.papers_total == 1


def test_cache_only_miss_does_not_enqueue(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = ArxivRequest(
        client_id="demo",
        search_query="cat:cs.AI",
        cache_policy=CachePolicy.CACHE_ONLY,
    )
    result = HuldraBroker(store=store, settings=settings).ensure(request)
    assert result.status == "cache_miss"
    assert store.status_summary().queue_depth_total == 0


def test_cache_or_enqueue_dedupes_queue_item(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    broker = HuldraBroker(store=store, settings=settings)
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI")
    first = broker.ensure(request)
    second = broker.ensure(request.model_copy(update={"client_id": "other"}))
    assert first.status == "queued"
    assert second.status == "queued"
    assert first.request_id == second.request_id
    assert first.cache_key == second.cache_key


def test_wait_until_ready_times_out_without_worker(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = ArxivRequest(
        client_id="demo",
        search_query="cat:cs.AI",
        cache_policy=CachePolicy.WAIT_UNTIL_READY,
        timeout_seconds=0.05,
    )
    result = HuldraBroker(store=store, settings=settings).ensure(request)
    assert result.status == "timeout"


def test_cooldown_state_is_exposed_while_request_is_enqueued(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    cooldown = utc_now() + timedelta(minutes=5)
    store.set_rate_state(RateState(cooldown_until=cooldown))
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI")
    result = HuldraBroker(store=store, settings=settings).ensure(request)
    assert result.status == "cooling_down"
    assert result.cooldown_until == cooldown
    assert store.status_summary().queue_depth_total == 1
