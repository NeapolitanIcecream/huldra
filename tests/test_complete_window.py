from __future__ import annotations

from dataclasses import dataclass

import pytest

from huldra.broker import HuldraBroker
from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.fetcher import FetchResult, NonRetryableFetchError
from huldra.models import ArxivRequest, CoverageStatus, LegacySyncMode
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
