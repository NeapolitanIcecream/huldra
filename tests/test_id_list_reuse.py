from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from huldra.broker import HuldraBroker
from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.fetcher import FetchResult
from huldra.models import ArxivRequest, CachePolicy, OaiRecord
from huldra.worker import HuldraWorker
from tests.conftest import make_paper


@dataclass
class CapturingFetcher:
    responses: list[FetchResult]
    seen: list[ArxivRequest]

    def fetch(self, request: ArxivRequest) -> FetchResult:
        self.seen.append(request)
        return self.responses.pop(0)


@dataclass
class CallbackFetcher:
    responses: list[FetchResult]
    seen: list[ArxivRequest]
    on_fetch: Callable[[], None] | None = None

    def fetch(self, request: ArxivRequest) -> FetchResult:
        self.seen.append(request)
        if self.on_fetch is not None:
            callback = self.on_fetch
            self.on_fetch = None
            callback()
        return self.responses.pop(0)


def test_pure_id_list_can_be_composed_from_paper_cache_in_request_order(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    store.upsert_papers([make_paper("2401.00002v1"), make_paper("2401.00001v1")])
    request = ArxivRequest(client_id="demo", id_list=("2401.00001v1", "2401.00002v1"))

    result = HuldraBroker(store=store, settings=settings).ensure(request)

    assert result.status == "ready"
    assert result.cache_hit
    assert result.request_id is None
    assert [paper.arxiv_id for paper in result.papers] == ["2401.00001v1", "2401.00002v1"]
    assert store.status_summary().queue_depth_total == 0


def test_pure_id_list_composition_ignores_repeated_ids_when_writing_cache(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    """Regression: repeated IDs produced duplicate cache_matches rows."""
    store.upsert_papers([make_paper("2401.00001v1")])
    request = ArxivRequest(client_id="demo", id_list=("2401.00001v1", "2401.00001v1"))

    result = HuldraBroker(store=store, settings=settings).ensure(request)

    assert result.status == "ready"
    assert result.cache_hit
    assert [paper.arxiv_id for paper in result.papers] == ["2401.00001v1"]
    assert result.papers_total == 1
    assert result.cached_papers_total == 1


def test_pure_id_list_composes_versioned_request_from_oai_base_row(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
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
    request = ArxivRequest(client_id="demo", id_list=("2401.00008v1",))

    result = HuldraBroker(store=store, settings=settings).ensure(request)

    assert result.status == "ready"
    assert result.cache_hit
    assert result.request_id is None
    assert [paper.arxiv_id for paper in result.papers] == ["2401.00008"]
    assert store.status_summary().queue_depth_total == 0


def test_id_list_worker_fetches_only_missing_ids_and_records_full_cache(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    store.upsert_papers([make_paper("2401.00001v1")])
    request = ArxivRequest(
        client_id="demo",
        id_list=("2401.00001v1", "2401.00002v1"),
        cache_policy=CachePolicy.WAIT_UNTIL_READY,
    )
    queued = HuldraBroker(store=store, settings=settings).ensure(
        request.model_copy(update={"cache_policy": CachePolicy.CACHE_OR_ENQUEUE})
    )
    fetcher = CapturingFetcher(
        responses=[FetchResult([make_paper("2401.00002v1")], total_results=1)],
        seen=[],
    )

    worker_result = HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()
    result = HuldraBroker(store=store, settings=settings).ensure(request)

    assert worker_result.status == "completed"
    assert [request.id_list for request in fetcher.seen] == [("2401.00002v1",)]
    assert result.status == "ready"
    assert queued.request_id is not None
    assert [paper.arxiv_id for paper in result.papers] == ["2401.00001v1", "2401.00002v1"]


def test_id_list_worker_deduplicates_repeated_ids_before_composed_cache_write(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    """Regression: repeated IDs in worker-composed caches violated cache_matches uniqueness."""
    request = ArxivRequest(
        client_id="demo",
        id_list=("2401.00001v1", "2401.00001v1"),
        cache_policy=CachePolicy.WAIT_UNTIL_READY,
    )
    store.enqueue_request(request)
    fetcher = CapturingFetcher(
        responses=[FetchResult([make_paper("2401.00001v1")], total_results=1)],
        seen=[],
    )

    worker_result = HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()
    result = HuldraBroker(store=store, settings=settings).ensure(request)

    assert worker_result.status == "completed"
    assert worker_result.papers_total == 1
    assert [seen.id_list for seen in fetcher.seen] == [("2401.00001v1",)]
    assert result.status == "ready"
    assert [paper.arxiv_id for paper in result.papers] == ["2401.00001v1"]


def test_id_list_worker_accepts_versioned_upstream_id_for_versionless_request(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    """Regression: versionless ID-list requests failed when arXiv returned a versioned ID."""
    request = ArxivRequest(
        client_id="demo",
        id_list=("2401.00001",),
        cache_policy=CachePolicy.WAIT_UNTIL_READY,
    )
    store.enqueue_request(request)
    fetcher = CapturingFetcher(
        responses=[FetchResult([make_paper("2401.00001v1")], total_results=1)],
        seen=[],
    )

    worker_result = HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()
    result = HuldraBroker(store=store, settings=settings).ensure(request)

    assert worker_result.status == "completed"
    assert [seen.id_list for seen in fetcher.seen] == [("2401.00001",)]
    assert result.status == "ready"
    assert [paper.arxiv_id for paper in result.papers] == ["2401.00001v1"]


def test_id_list_worker_deduplicates_versionless_alias_before_composed_cache_write(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    """Regression: versionless and versioned aliases could write duplicate cache matches."""
    request = ArxivRequest(
        client_id="demo",
        id_list=("2401.00001", "2401.00001v1"),
        cache_policy=CachePolicy.WAIT_UNTIL_READY,
    )
    store.enqueue_request(request)
    fetcher = CapturingFetcher(
        responses=[FetchResult([make_paper("2401.00001v1")], total_results=1)],
        seen=[],
    )

    worker_result = HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()
    result = HuldraBroker(store=store, settings=settings).ensure(request)

    assert worker_result.status == "completed"
    assert result.status == "ready"
    assert [paper.arxiv_id for paper in result.papers] == ["2401.00001v1"]


def test_reordered_cold_id_list_requests_do_not_duplicate_upstream_fetches(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    first = ArxivRequest(client_id="a", id_list=("2401.00001v1", "2401.00002v1"))
    second = ArxivRequest(client_id="b", id_list=("2401.00002v1", "2401.00001v1"))
    HuldraBroker(store=store, settings=settings).ensure(first)
    HuldraBroker(store=store, settings=settings).ensure(second)
    fetcher = CapturingFetcher(
        responses=[FetchResult([make_paper("2401.00001v1"), make_paper("2401.00002v1")], total_results=2)],
        seen=[],
    )

    first_pass = HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()
    second_pass = HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()

    assert first_pass.status == "completed"
    assert second_pass.status == "cache_hit"

    assert len(fetcher.seen) == 1
    second_result = HuldraBroker(store=store, settings=settings).ensure(second)
    assert [paper.arxiv_id for paper in second_result.papers] == ["2401.00002v1", "2401.00001v1"]


def test_overlapping_cold_id_list_waiter_uses_warmed_cache_without_reservation_expiry(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    """Regression: reservation waiters stayed delayed until the reservation TTL."""
    first = ArxivRequest(client_id="a", id_list=("2401.00001v1", "2401.00002v1"))
    second = ArxivRequest(client_id="b", id_list=("2401.00002v1", "2401.00001v1"))
    broker = HuldraBroker(store=store, settings=settings)
    broker.ensure(first)
    queued_second = broker.ensure(second)
    assert queued_second.request_id is not None
    second_request_id = queued_second.request_id

    blocked_result = None
    blocked_item = None

    def block_second_while_first_reservation_is_active() -> None:
        nonlocal blocked_result, blocked_item
        blocked_result = HuldraWorker(
            store,
            settings,
            fetcher=CapturingFetcher(responses=[], seen=[]),
            owner_token="second-worker",
            sleep=lambda _: None,
        ).run_once()
        blocked_item = store.get_queue_item(second_request_id)

    fetcher = CallbackFetcher(
        responses=[
            FetchResult(
                [make_paper("2401.00001v1"), make_paper("2401.00002v1")],
                total_results=2,
            )
        ],
        seen=[],
        on_fetch=block_second_while_first_reservation_is_active,
    )

    first_pass = HuldraWorker(
        store,
        settings,
        fetcher=fetcher,
        owner_token="first-worker",
        sleep=lambda _: None,
    ).run_once()
    wait_result = HuldraBroker(store=store, settings=settings, fetcher=fetcher).sync_windows(
        [second],
        wait=True,
        wait_timeout_seconds=0.05,
    )

    assert first_pass.status == "completed"
    assert blocked_result is not None
    assert blocked_result.status == "blocked"
    assert blocked_item is not None
    assert blocked_item.status == "delayed"
    assert blocked_item.error_category == "id_fetch_reserved"
    assert len(fetcher.seen) == 1
    assert wait_result.completed_windows_total == 1
    assert wait_result.upstream_requests_total == 0
    assert wait_result.requests[0].raw_cache_status == "completed"
    result = HuldraBroker(store=store, settings=settings).ensure(second)
    assert [paper.arxiv_id for paper in result.papers] == ["2401.00002v1", "2401.00001v1"]


def test_pure_id_list_reuse_preserves_old_style_slash_ids(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    store.upsert_papers([make_paper("hep-th/9901001v1")])
    request = ArxivRequest(client_id="demo", id_list=("hep-th/9901001v1",))

    result = HuldraBroker(store=store, settings=settings).ensure(request)

    assert result.status == "ready"
    assert result.papers[0].arxiv_id == "hep-th/9901001v1"


def test_pure_id_list_rejects_max_results_smaller_than_requested_ids() -> None:
    with pytest.raises(ValueError, match="max_results"):
        ArxivRequest(
            client_id="demo",
            id_list=("2401.00001v1", "2401.00002v1"),
            max_results=1,
        )


def test_mixed_query_and_id_list_does_not_use_paper_cache_shortcut(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    store.upsert_papers([make_paper("2401.00001v1")])
    request = ArxivRequest(
        client_id="demo",
        search_query="cat:cs.AI",
        id_list=("2401.00001v1",),
    )

    result = HuldraBroker(store=store, settings=settings).ensure(request)

    assert result.status == "queued"
    assert result.request_id is not None
