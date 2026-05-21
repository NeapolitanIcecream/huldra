from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from huldra.broker import HuldraBroker
from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.fetcher import FetchResult, RateLimitedError
from huldra.models import ArxivRequest, CachePolicy, RateState, ReadinessMode
from huldra.planner import build_submitted_date_windows
from huldra.time import utc_now
from tests.conftest import make_paper


@dataclass
class FakeFetcher:
    responses: list[FetchResult | Exception]
    calls: int = 0

    def fetch(self, request: ArxivRequest) -> FetchResult:
        self.calls += 1
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def test_build_submitted_date_windows_plans_inclusive_daily_ranges() -> None:
    requests = build_submitted_date_windows(
        search_queries=["cat:cs.AI", "cat:cs.LG"],
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 2),
        max_results=60,
        client_id="recoleta:test",
    )

    assert len(requests) == 4
    assert requests[0].submitted_start == datetime(2026, 1, 1, tzinfo=UTC)
    assert requests[0].submitted_end == datetime(2026, 1, 2, tzinfo=UTC)
    assert requests[-1].submitted_start == datetime(2026, 1, 2, tzinfo=UTC)
    assert requests[-1].submitted_end == datetime(2026, 1, 3, tzinfo=UTC)


def test_sync_windows_wait_coerces_cache_only_and_completes_raw_cache(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    request = ArxivRequest(
        client_id="recoleta:test",
        search_query="cat:cs.AI",
        submitted_start=today,
        submitted_end=today + timedelta(days=1),
        cache_policy=CachePolicy.CACHE_ONLY,
        readiness=ReadinessMode.ANALYSIS_READY,
        timeout_seconds=1,
    )
    fetcher = FakeFetcher([FetchResult([make_paper()], total_results=1)])
    broker = HuldraBroker(store=store, settings=settings, fetcher=fetcher)

    result = broker.sync_windows([request], wait=True)

    assert result.requested_total == 1
    assert result.cache_miss_total == 1
    assert result.queued_total == 1
    assert result.completed_windows_total == 1
    assert result.upstream_requests_total == 1
    assert result.papers_total == 1
    assert result.requests[0].raw_cache_status == "completed"
    assert result.requests[0].serving_status == "immature"


def test_sync_windows_wait_ignores_unrelated_queued_items(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    unrelated = store.enqueue_request(
        ArxivRequest(client_id="other", search_query="cat:cs.LG", priority=100)
    )
    target = ArxivRequest(
        client_id="recoleta:test",
        search_query="cat:cs.AI",
        timeout_seconds=1,
    )
    fetcher = FakeFetcher([FetchResult([make_paper("2401.00002v1")], total_results=1)])
    broker = HuldraBroker(store=store, settings=settings, fetcher=fetcher)

    result = broker.sync_windows([target], wait=True)

    assert result.completed_windows_total == 1
    assert result.upstream_requests_total == 1
    assert fetcher.calls == 1
    still_unrelated = store.get_queue_item(unrelated.request_id)
    assert still_unrelated is not None
    assert still_unrelated.status == "queued"


def test_sync_windows_reports_joined_existing_queue_without_counting_upstream(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    target = ArxivRequest(client_id="recoleta:test", search_query="cat:cs.AI")
    store.enqueue_request(target)
    broker = HuldraBroker(store=store, settings=settings)

    result = broker.sync_windows([target], wait=False)

    assert result.queued_total == 1
    assert result.upstream_requests_total == 0
    assert result.requests[0].joined_existing_queue


def test_sync_windows_wait_returns_when_global_cooldown_blocks_target(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    cooldown_until = utc_now() + timedelta(seconds=30)
    store.set_rate_state(RateState(cooldown_until=cooldown_until))
    target = ArxivRequest(
        client_id="recoleta:test",
        search_query="cat:cs.AI",
        timeout_seconds=1,
    )
    fetcher = FakeFetcher([])
    broker = HuldraBroker(store=store, settings=settings, fetcher=fetcher)

    result = broker.sync_windows([target], wait=True)

    assert result.cooldown_active
    assert result.cooldown_active_total == 1
    assert result.skipped_windows_total == 1
    assert result.requests[0].raw_cache_status == "skipped"
    assert result.requests[0].cooldown_until == cooldown_until
    assert fetcher.calls == 0


def test_sync_windows_wait_reports_inline_429_retry_after(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    target = ArxivRequest(
        client_id="recoleta:test",
        search_query="cat:cs.AI",
        timeout_seconds=1,
    )
    fetcher = FakeFetcher([RateLimitedError(17)])
    broker = HuldraBroker(store=store, settings=settings, fetcher=fetcher)

    result = broker.sync_windows([target], wait=True)

    assert result.upstream_requests_total == 1
    assert result.upstream_429_total == 1
    assert result.retry_after_seconds == 17
    assert result.rate_limited_windows_total == 1
    assert result.requests[0].raw_cache_status == "rate_limited"
