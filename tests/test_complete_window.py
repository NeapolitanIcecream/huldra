from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest

import huldra.broker as broker_module
import huldra.db as db_module
import huldra.limiter as limiter_module
import huldra.worker as worker_module
from huldra.broker import HuldraBroker
from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.fetcher import FetchResult, NonRetryableFetchError, TransientFetchError
from huldra.keys import request_cache_key
from huldra.models import (
    ArxivRequest,
    CoverageStatus,
    LegacySyncMode,
    QueueItem,
    QueueWorkKind,
)
from huldra.worker import HuldraWorker
from tests.conftest import make_paper


@dataclass
class CapturingFetcher:
    responses: list[FetchResult | Exception]
    seen: list[ArxivRequest]

    def fetch(self, request: ArxivRequest) -> FetchResult:
        self.seen.append(request)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def test_default_sync_reports_overflowing_legacy_response_as_slice(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI", max_results=1)
    fetcher = CapturingFetcher([FetchResult([make_paper("2401.00001v1")], total_results=3)], [])

    result = HuldraBroker(store=store, settings=settings, fetcher=fetcher).sync_windows(
        [request],
        wait=True,
    )

    assert result.completed_windows_total == 1
    assert result.completed_slices_total == 1
    assert result.requests[0].coverage_status == CoverageStatus.SLICE
    assert result.requests[0].total_results == 3
    assert [seen.start for seen in fetcher.seen] == [0]


def test_complete_window_sync_fetches_all_contiguous_pages(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI", max_results=1)
    fetcher = CapturingFetcher(
        [
            FetchResult([make_paper("2401.00001v1")], total_results=3),
            FetchResult([make_paper("2401.00002v1")], total_results=3),
            FetchResult([make_paper("2401.00003v1")], total_results=3),
        ],
        [],
    )

    result = HuldraBroker(store=store, settings=settings, fetcher=fetcher).sync_windows(
        [request],
        wait=True,
        wait_timeout_seconds=10,
        mode=LegacySyncMode.COMPLETE_WINDOW,
    )

    assert result.completed_windows_total == 1
    assert result.complete_windows_total == 1
    assert result.completed_slices_total == 3
    assert result.papers_total == 3
    assert result.requests[0].coverage_status == CoverageStatus.COMPLETE
    assert result.requests[0].pages_total == 3
    assert result.requests[0].pages_completed_total == 3
    assert [seen.start for seen in fetcher.seen] == [0, 1, 2]


def test_complete_window_counts_duplicate_misses_as_one_request(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = ArxivRequest(client_id="first", search_query="cat:cs.AI", max_results=1)
    duplicate = request.model_copy(update={"client_id": "second"})
    fetcher = CapturingFetcher(
        [FetchResult([make_paper("2401.00001v1")], total_results=1)],
        [],
    )

    result = HuldraBroker(store=store, settings=settings, fetcher=fetcher).sync_windows(
        [request, duplicate],
        wait=True,
        wait_timeout_seconds=4,
        mode=LegacySyncMode.COMPLETE_WINDOW,
        max_pages_per_window=1,
        max_requests_total=1,
    )

    assert result.requested_total == 2
    assert result.completed_windows_total == 2
    assert result.complete_windows_total == 2
    assert result.upstream_requests_total == 1
    assert len(fetcher.seen) == 1


def test_sync_request_admission_excludes_id_lists_composed_from_paper_store(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    store.upsert_papers([make_paper("2401.00001v1"), make_paper("2401.00002v1")])
    requests = [
        ArxivRequest(client_id="first", id_list=("2401.00001v1",)),
        ArxivRequest(client_id="second", id_list=("2401.00002v1",)),
    ]
    fetcher = CapturingFetcher([], [])

    result = HuldraBroker(store=store, settings=settings, fetcher=fetcher).sync_windows(
        requests,
        wait=True,
        max_requests_total=1,
    )

    assert result.completed_windows_total == 2
    assert result.cache_hit_total == 2
    assert result.upstream_requests_total == 0
    assert fetcher.seen == []


def test_complete_window_cached_first_page_allows_immediate_followup(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    tuned = settings.model_copy(update={"request_interval_seconds": 5.0})
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI", max_results=1)
    store.record_completed_cache_entry(
        cache_key=request_cache_key(request),
        request=request,
        papers=[make_paper("2401.00001v1")],
        total_results=2,
    )
    fetcher = CapturingFetcher(
        [FetchResult([make_paper("2401.00002v1")], total_results=2)],
        [],
    )

    result = HuldraBroker(store=store, settings=tuned, fetcher=fetcher).sync_windows(
        [request],
        wait=True,
        wait_timeout_seconds=4,
        mode=LegacySyncMode.COMPLETE_WINDOW,
        max_pages_per_window=2,
        max_requests_total=1,
    )

    assert result.complete_windows_total == 1
    assert result.upstream_requests_total == 1
    assert [seen.start for seen in fetcher.seen] == [1]


def test_complete_window_caps_atom_request_to_remaining_runtime(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
    monkeypatch.setattr(broker_module.time, "monotonic", lambda: 0.0)
    monkeypatch.setattr(broker_module, "utc_now", lambda: started)
    monkeypatch.setattr(db_module, "utc_now", lambda: started)
    monkeypatch.setattr(limiter_module, "utc_now", lambda: started)
    monkeypatch.setattr(worker_module, "utc_now", lambda: started)
    tuned = settings.model_copy(update={"request_timeout_seconds": 30.0})
    fetcher = CapturingFetcher(
        [FetchResult([make_paper("2401.00001v1")], total_results=1)],
        [],
    )

    result = HuldraBroker(store=store, settings=tuned, fetcher=fetcher).sync_windows(
        [ArxivRequest(client_id="demo", search_query="cat:cs.AI", max_results=1)],
        wait=True,
        wait_timeout_seconds=4,
        mode=LegacySyncMode.COMPLETE_WINDOW,
        max_pages_per_window=1,
        max_requests_total=1,
    )

    assert result.completed_windows_total == 1
    assert fetcher.seen[0].timeout_seconds == 4.0


def test_complete_window_sync_requires_wait_true(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI", max_results=1)

    with pytest.raises(ValueError, match="requires wait=True"):
        HuldraBroker(store=store, settings=settings).sync_windows(
            [request],
            mode=LegacySyncMode.COMPLETE_WINDOW,
            wait=False,
        )

    with store.connect() as conn:
        jobs_total = conn.execute("SELECT COUNT(*) FROM sync_jobs").fetchone()[0]
    assert jobs_total == 0


def test_complete_window_uses_result_count_for_first_followup_offset(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI", max_results=2)
    fetcher = CapturingFetcher(
        [
            FetchResult([make_paper("2401.00001v1")], total_results=3),
            FetchResult(
                [make_paper("2401.00002v1"), make_paper("2401.00003v1")],
                total_results=3,
            ),
        ],
        [],
    )

    result = HuldraBroker(store=store, settings=settings, fetcher=fetcher).sync_windows(
        [request],
        wait=True,
        wait_timeout_seconds=10,
        mode=LegacySyncMode.COMPLETE_WINDOW,
    )

    assert result.complete_windows_total == 1
    assert result.requests[0].coverage_status == CoverageStatus.COMPLETE
    assert result.requests[0].pages_total == 2
    assert [seen.start for seen in fetcher.seen] == [0, 1]


def test_complete_window_rejects_cross_page_duplicate_papers(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI", max_results=1)
    fetcher = CapturingFetcher(
        [
            FetchResult([make_paper("2401.00001v1")], total_results=3),
            FetchResult([make_paper("2401.00001v1")], total_results=3),
            FetchResult([make_paper("2401.00003v1")], total_results=3),
        ],
        [],
    )

    result = HuldraBroker(store=store, settings=settings, fetcher=fetcher).sync_windows(
        [request],
        wait=True,
        wait_timeout_seconds=10,
        mode=LegacySyncMode.COMPLETE_WINDOW,
    )

    assert result.completed_windows_total == 0
    assert result.partial_windows_total == 1
    assert result.requests[0].coverage_status == CoverageStatus.PARTIAL
    assert result.requests[0].result_count == 2
    assert result.requests[0].error_category == "overlapping_page_results"
    assert result.requests[0].pages_completed_total == 3


def test_complete_window_overflow_does_not_fetch_followup_pages(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    settings = settings.model_copy(update={"legacy_search_window_result_cap": 2})
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI", max_results=1)
    fetcher = CapturingFetcher([FetchResult([make_paper("2401.00001v1")], total_results=3)], [])

    result = HuldraBroker(store=store, settings=settings, fetcher=fetcher).sync_windows(
        [request],
        wait=True,
        wait_timeout_seconds=10,
        mode=LegacySyncMode.COMPLETE_WINDOW,
    )

    assert result.completed_windows_total == 0
    assert result.overflow_windows_total == 1
    assert result.requests[0].raw_cache_status == "overflow"
    assert result.requests[0].coverage_status == CoverageStatus.OVERFLOW
    assert result.requests[0].error_category == "legacy_window_overflow"
    assert [seen.start for seen in fetcher.seen] == [0]


def test_complete_window_failed_middle_page_keeps_window_partial(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI", max_results=1)
    fetcher = CapturingFetcher(
        [
            FetchResult([make_paper("2401.00001v1")], total_results=3),
            NonRetryableFetchError("bad page", status_code=400),
            FetchResult([make_paper("2401.00003v1")], total_results=3),
        ],
        [],
    )

    result = HuldraBroker(store=store, settings=settings, fetcher=fetcher).sync_windows(
        [request],
        wait=True,
        wait_timeout_seconds=10,
        mode=LegacySyncMode.COMPLETE_WINDOW,
    )

    assert result.completed_windows_total == 0
    assert result.partial_windows_total == 1
    assert result.completed_slices_total == 2
    assert result.requests[0].raw_cache_status == "partial"
    assert result.requests[0].coverage_status == CoverageStatus.PARTIAL
    assert result.requests[0].pages_total == 3
    assert result.requests[0].pages_completed_total == 2
    assert [seen.start for seen in fetcher.seen] == [0, 1, 2]


def test_complete_window_page_budget_stops_before_followup_enqueue(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    """A tiny first page must not fan out into thousands of durable requests."""
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI", max_results=1)
    fetcher = CapturingFetcher(
        [FetchResult([make_paper("2401.00001v1")], total_results=9_999)],
        [],
    )

    result = HuldraBroker(store=store, settings=settings, fetcher=fetcher).sync_windows(
        [request],
        wait=True,
        wait_timeout_seconds=30,
        mode=LegacySyncMode.COMPLETE_WINDOW,
        max_pages_per_window=10,
        max_requests_total=10,
    )

    assert [seen.start for seen in fetcher.seen] == [0]
    assert result.requests[0].raw_cache_status == "partial"
    assert result.requests[0].error_category == "page_budget_exceeded"
    assert store.status_summary().queue_depth_total == 0
    with store.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM sync_job_pages").fetchone()[0] == 1


def test_complete_window_deadline_stops_before_followup_enqueue(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI", max_results=1)
    fetcher = CapturingFetcher(
        [FetchResult([make_paper("2401.00001v1")], total_results=3)],
        [],
    )

    result = HuldraBroker(store=store, settings=settings, fetcher=fetcher).sync_windows(
        [request],
        wait=True,
        wait_timeout_seconds=1,
        mode=LegacySyncMode.COMPLETE_WINDOW,
        max_pages_per_window=10,
        max_requests_total=10,
    )

    assert [seen.start for seen in fetcher.seen] == [0]
    assert result.requests[0].error_category == "deadline_budget_exceeded"
    assert store.status_summary().queue_depth_total == 0


def test_backfill_request_budget_rejects_before_creating_jobs(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    broker = HuldraBroker(store=store, settings=settings)

    with pytest.raises(ValueError, match="request budget"):
        broker.backfill_windows(
            search_queries=["cat:cs.AI", "cat:cs.LG"],
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 3),
            max_results=1,
            wait=True,
            mode=LegacySyncMode.COMPLETE_WINDOW,
            max_requests_total=5,
        )

    with store.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM sync_jobs").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM queue_items").fetchone()[0] == 0


def test_complete_window_rechecks_deadline_before_initial_enqueue(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI", max_results=1)
    monotonic_now = [0.0]
    monkeypatch.setattr(broker_module.time, "monotonic", lambda: monotonic_now[0])
    original_create = store.create_sync_job

    def create_after_deadline(
        job_request: ArxivRequest,
        job_mode: LegacySyncMode,
        *,
        upstream_budget_id: str | None = None,
    ) -> str:
        sync_job_id = original_create(
            job_request,
            job_mode,
            upstream_budget_id=upstream_budget_id,
        )
        monotonic_now[0] = 2.0
        return sync_job_id

    monkeypatch.setattr(store, "create_sync_job", create_after_deadline)

    result = HuldraBroker(store=store, settings=settings).sync_windows(
        [request],
        wait=True,
        wait_timeout_seconds=1,
        mode=LegacySyncMode.COMPLETE_WINDOW,
        max_pages_per_window=1,
        max_requests_total=1,
    )

    assert result.requests[0].error_category == "deadline_budget_exceeded"
    with store.connect() as conn:
        active = conn.execute(
            "SELECT COUNT(*) FROM queue_items "
            "WHERE status IN ('queued', 'delayed', 'claimed')"
        ).fetchone()[0]
    assert active == 0


def test_complete_window_rechecks_deadline_after_rate_wait_before_network(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wall_now = [datetime(2026, 7, 22, 12, 0, tzinfo=UTC)]
    elapsed = [0.0]

    def now() -> datetime:
        return wall_now[0]

    def monotonic() -> float:
        return elapsed[0]

    def oversleep(seconds: float) -> None:
        wall_now[0] += timedelta(seconds=seconds + 2)
        elapsed[0] += seconds + 2

    monkeypatch.setattr(broker_module.time, "monotonic", monotonic)
    monkeypatch.setattr(broker_module.time, "sleep", oversleep)
    monkeypatch.setattr(broker_module, "utc_now", now)
    monkeypatch.setattr(db_module, "utc_now", now)
    monkeypatch.setattr(limiter_module, "utc_now", now)
    monkeypatch.setattr(worker_module, "utc_now", now)
    store.set_rate_state(store.get_rate_state().model_copy(update={"last_request_at": now()}))
    fetcher = CapturingFetcher([FetchResult([make_paper()], total_results=1)], [])

    result = HuldraBroker(store=store, settings=settings, fetcher=fetcher).sync_windows(
        [ArxivRequest(client_id="demo", search_query="cat:cs.AI", max_results=1)],
        wait=True,
        wait_timeout_seconds=4,
        mode=LegacySyncMode.COMPLETE_WINDOW,
        max_pages_per_window=1,
        max_requests_total=1,
    )

    assert fetcher.seen == []
    assert result.upstream_requests_total == 0
    assert result.requests[0].error_category == "deadline_budget_exceeded"


def test_complete_window_request_budget_caps_retried_upstream_attempts(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI", max_results=1)
    first_fetcher = CapturingFetcher([TransientFetchError("temporary", status_code=500)], [])

    HuldraBroker(store=store, settings=settings, fetcher=first_fetcher).sync_windows(
        [request],
        wait=True,
        wait_timeout_seconds=30,
        mode=LegacySyncMode.COMPLETE_WINDOW,
        max_pages_per_window=1,
        max_requests_total=1,
    )
    assert len(first_fetcher.seen) == 1

    old = "2000-01-01T00:00:00+00:00"
    with store.begin_immediate() as conn:
        conn.execute(
            "UPDATE queue_items SET next_attempt_at = ? WHERE status = 'delayed'",
            (old,),
        )
        conn.execute(
            "UPDATE rate_state SET last_request_at = ?, last_request_started_at = ? "
            "WHERE name = 'arxiv_legacy_api'",
            (old, old),
        )
    retry_fetcher = CapturingFetcher([FetchResult([make_paper()], total_results=1)], [])

    retry = HuldraWorker(store, settings, fetcher=retry_fetcher).run_once()

    assert retry.status == "budget_exceeded"
    assert retry.error_category == "request_budget_exceeded"
    assert retry_fetcher.seen == []
    entry = store.get_cache_entry(request_cache_key(request))
    assert entry is not None
    assert entry.upstream_requests_total == 1


def test_complete_window_joined_queue_item_consumes_maintenance_budget(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI", max_results=1)
    existing = store.enqueue_request(request)
    fetcher = CapturingFetcher([FetchResult([make_paper()], total_results=1)], [])

    result = HuldraBroker(store=store, settings=settings, fetcher=fetcher).sync_windows(
        [request],
        wait=True,
        wait_timeout_seconds=30,
        mode=LegacySyncMode.COMPLETE_WINDOW,
        max_pages_per_window=1,
        max_requests_total=1,
    )

    assert result.completed_windows_total == 1
    assert len(fetcher.seen) == 1
    with store.connect() as conn:
        budget = conn.execute(
            """
            SELECT budget.budget_id, budget.requests_started
            FROM sync_jobs AS job
            JOIN upstream_request_budgets AS budget
              ON budget.budget_id = job.upstream_budget_id
            """
        ).fetchone()
        membership = conn.execute(
            """
            SELECT budget_id, last_charged_attempt
            FROM queue_item_upstream_budgets
            WHERE request_id=?
            """,
            (existing.request_id,),
        ).fetchone()
    assert tuple(budget) == (membership["budget_id"], 1)
    assert membership["last_charged_attempt"] == 1


def test_complete_window_deadline_is_reported_when_shared_item_is_preserved(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = ArxivRequest(client_id="ordinary", search_query="cat:cs.AI", max_results=1)
    ordinary_item = store.enqueue_request(request)
    assert store.acquire_lease("upstream_fetch", "other-worker", 60)
    elapsed = [0.0]

    def advance_past_deadline(_seconds: float) -> None:
        elapsed[0] = 2.0

    monkeypatch.setattr(broker_module.time, "monotonic", lambda: elapsed[0])
    monkeypatch.setattr(broker_module.time, "sleep", advance_past_deadline)

    result = HuldraBroker(store=store, settings=settings).sync_windows(
        [request],
        wait=True,
        wait_timeout_seconds=1,
        mode=LegacySyncMode.COMPLETE_WINDOW,
        max_pages_per_window=1,
        max_requests_total=1,
    )

    request_result = result.requests[0]
    sync_job = store.get_sync_job(request_result.sync_job_id or "")
    preserved = store.get_queue_item(ordinary_item.request_id)
    assert request_result.error_category == "deadline_budget_exceeded"
    assert result.budget_exhausted_windows_total == 1
    assert sync_job is not None
    assert sync_job["error_category"] == "deadline_budget_exceeded"
    assert preserved is not None
    assert preserved.status == "delayed"
    assert preserved.upstream_budget_id is None


def test_complete_window_rechecks_deadline_before_each_followup_enqueue(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI", max_results=1)
    store.record_completed_cache_entry(
        cache_key=request_cache_key(request),
        request=request,
        papers=[make_paper()],
        total_results=3,
    )
    monotonic_now = [0.0]
    monkeypatch.setattr(broker_module.time, "monotonic", lambda: monotonic_now[0])
    original_record_page = store.record_sync_job_page
    original_enqueue = store.enqueue_request_for_work
    enqueued_at: list[tuple[int, float]] = []

    def record_page_then_expire(
        *,
        sync_job_id: str,
        request: ArxivRequest,
        cache_key: str,
        status: str,
        result_count: int = 0,
        total_results: int | None = None,
        diagnostics: dict[str, Any] | None = None,
    ) -> None:
        original_record_page(
            sync_job_id=sync_job_id,
            request=request,
            cache_key=cache_key,
            status=status,
            result_count=result_count,
            total_results=total_results,
            diagnostics=diagnostics,
        )
        if request.start > 0:
            monotonic_now[0] = 20.0

    def capture_enqueue(
        request: ArxivRequest,
        cache_key: str | None = None,
        *,
        work_kind: QueueWorkKind | None = None,
        upstream_budget_id: str | None = None,
    ) -> tuple[QueueItem, bool]:
        enqueued_at.append((request.start, monotonic_now[0]))
        return original_enqueue(
            request,
            cache_key,
            work_kind=work_kind,
            upstream_budget_id=upstream_budget_id,
        )

    monkeypatch.setattr(store, "record_sync_job_page", record_page_then_expire)
    monkeypatch.setattr(store, "enqueue_request_for_work", capture_enqueue)

    result = HuldraBroker(store=store, settings=settings).sync_windows(
        [request],
        wait=True,
        wait_timeout_seconds=10,
        mode=LegacySyncMode.COMPLETE_WINDOW,
        max_pages_per_window=10,
        max_requests_total=10,
    )

    assert enqueued_at == []
    assert result.requests[0].error_category == "deadline_budget_exceeded"
    with store.connect() as conn:
        pages = conn.execute(
            "SELECT start, status FROM sync_job_pages ORDER BY start"
        ).fetchall()
    assert [tuple(page) for page in pages] == [(0, "completed")]


def test_complete_window_rechecks_deadline_after_budget_reservation_before_network(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wall_now = [datetime(2026, 7, 22, 12, 0, tzinfo=UTC)]
    elapsed = [0.0]

    def now() -> datetime:
        return wall_now[0]

    monkeypatch.setattr(broker_module.time, "monotonic", lambda: elapsed[0])
    monkeypatch.setattr(broker_module, "utc_now", now)
    monkeypatch.setattr(db_module, "utc_now", now)
    monkeypatch.setattr(limiter_module, "utc_now", now)
    monkeypatch.setattr(worker_module, "utc_now", now)
    original_reserve = store.reserve_queue_item_upstream_request

    def reserve_then_expire(
        request_id: str,
        *,
        now: datetime | None = None,
    ) -> str | None:
        outcome = original_reserve(request_id, now=now)
        wall_now[0] += timedelta(seconds=5)
        elapsed[0] += 5
        return outcome

    monkeypatch.setattr(store, "reserve_queue_item_upstream_request", reserve_then_expire)
    fetcher = CapturingFetcher([FetchResult([make_paper()], total_results=1)], [])

    result = HuldraBroker(store=store, settings=settings, fetcher=fetcher).sync_windows(
        [ArxivRequest(client_id="demo", search_query="cat:cs.AI", max_results=1)],
        wait=True,
        wait_timeout_seconds=4,
        mode=LegacySyncMode.COMPLETE_WINDOW,
        max_pages_per_window=1,
        max_requests_total=1,
    )

    assert fetcher.seen == []
    assert result.upstream_requests_total == 0
    assert result.requests[0].error_category == "deadline_budget_exceeded"
