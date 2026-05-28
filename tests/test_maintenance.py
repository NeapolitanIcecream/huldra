from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import pytest

from huldra.broker import HuldraBroker
from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.fetcher import FetchResult, NonRetryableFetchError, RateLimitedError
from huldra.keys import request_cache_key
from huldra.models import ArxivRequest, CachePolicy, CoverageStatus, RateState, ReadinessMode
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


def _corrupt_completed_cache(store: HuldraStore, request: ArxivRequest) -> str:
    key = request_cache_key(request)
    store.record_completed_cache_entry(cache_key=key, request=request, papers=[make_paper()])
    with store.begin_immediate() as conn:
        conn.execute("DELETE FROM cache_matches WHERE cache_key = ?", (key,))
    return key


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


def test_sync_windows_async_cache_miss_completes_sync_job_as_queued(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    target = ArxivRequest(client_id="recoleta:test", search_query="cat:cs.AI")
    broker = HuldraBroker(store=store, settings=settings)

    result = broker.sync_windows([target], wait=False)

    sync_job_id = result.requests[0].sync_job_id
    assert sync_job_id is not None
    job = store.get_sync_job(sync_job_id)
    assert job is not None
    assert job["status"] == "queued"
    assert job["coverage_status"] == CoverageStatus.UNKNOWN
    assert job["pages_total"] == 1
    assert job["pages_completed_total"] == 0
    with store.connect() as conn:
        page = conn.execute(
            "SELECT status FROM sync_job_pages WHERE sync_job_id = ?",
            (sync_job_id,),
        ).fetchone()
        sync_job_row = conn.execute(
            "SELECT completed_at FROM sync_jobs WHERE sync_job_id = ?",
            (sync_job_id,),
        ).fetchone()
    assert page is not None
    assert page["status"] == "queued"
    assert sync_job_row is not None
    assert sync_job_row["completed_at"] is not None


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


def test_sync_windows_wait_preserves_limiter_delay_between_inline_requests(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: inline maintenance drains must honor worker limiter sleeps."""
    sleeps: list[float] = []

    def record_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr("huldra.worker.time.sleep", record_sleep)
    first = ArxivRequest(client_id="recoleta:test", search_query="cat:cs.AI")
    second = ArxivRequest(client_id="recoleta:test", search_query="cat:cs.LG")
    fetcher = FakeFetcher(
        [
            FetchResult([make_paper("2401.00001v1")], total_results=1),
            FetchResult([make_paper("2401.00002v1")], total_results=1),
        ]
    )
    broker = HuldraBroker(store=store, settings=settings, fetcher=fetcher)

    result = broker.sync_windows([first, second], wait=True, wait_timeout_seconds=10)

    limiter_sleeps = [
        seconds for seconds in sleeps if seconds >= settings.request_interval_seconds - 0.5
    ]
    assert result.completed_windows_total == 2
    assert result.upstream_requests_total == 2
    assert fetcher.calls == 2
    assert limiter_sleeps


def test_sync_windows_reports_queued_retry_for_unreadable_completed_cache(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    """Regression: unreadable completed caches must surface pending retry state."""
    target = ArxivRequest(client_id="recoleta:test", search_query="cat:cs.AI")
    key = _corrupt_completed_cache(store, target)
    broker = HuldraBroker(store=store, settings=settings)

    result = broker.sync_windows([target], wait=False)

    assert result.completed_windows_total == 0
    assert result.queued_total == 1
    assert result.requests[0].cache_key == key
    assert result.requests[0].raw_cache_status == "queued"
    assert result.requests[0].serving_status == "queued"


def test_sync_windows_wait_reports_failed_retry_for_unreadable_completed_cache(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    """Regression: failed retries for corrupt completed caches must not look completed."""
    target = ArxivRequest(
        client_id="recoleta:test",
        search_query="cat:cs.AI",
        timeout_seconds=1,
    )
    key = _corrupt_completed_cache(store, target)
    fetcher = FakeFetcher([NonRetryableFetchError("arXiv API returned HTTP 404", status_code=404)])
    broker = HuldraBroker(store=store, settings=settings, fetcher=fetcher)

    result = broker.sync_windows([target], wait=True)

    assert result.completed_windows_total == 0
    assert result.failed_windows_total == 1
    assert result.upstream_requests_total == 1
    assert result.requests[0].cache_key == key
    assert result.requests[0].raw_cache_status == "failed"
    assert result.requests[0].serving_status == "failed"
    assert result.requests[0].error_category == "non_retryable"
