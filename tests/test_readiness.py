from __future__ import annotations

from datetime import UTC, datetime, timedelta

from huldra.broker import HuldraBroker
from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.keys import request_cache_key
from huldra.models import ArxivRequest, CachePolicy, ReadinessMode
from tests.conftest import make_paper


def _record(store: HuldraStore, request: ArxivRequest) -> str:
    key = request_cache_key(request)
    store.record_completed_cache_entry(
        cache_key=key,
        request=request,
        papers=[make_paper()],
    )
    return key


def test_current_utc_day_completed_cache_is_not_analysis_ready(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    now = datetime.now(UTC)
    start = datetime(now.year, now.month, now.day, tzinfo=UTC)
    request = ArxivRequest(
        client_id="demo",
        search_query="cat:cs.AI",
        submitted_start=start,
        submitted_end=start + timedelta(days=1),
        readiness=ReadinessMode.ANALYSIS_READY,
    )
    _record(store, request)
    result = HuldraBroker(store=store, settings=settings).ensure(request)
    assert result.status == "immature"
    assert not result.ready
    assert not result.analysis_ready
    assert result.cache_readable
    assert not result.mature
    assert result.papers == []
    assert result.papers_total == 0
    assert result.cached_papers_total == 1
    assert result.blocked_reason == "immature_window"


def test_yesterday_utc_day_completed_cache_is_analysis_ready(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start = today - timedelta(days=1)
    request = ArxivRequest(
        client_id="demo",
        search_query="cat:cs.AI",
        submitted_start=start,
        submitted_end=today,
        readiness=ReadinessMode.ANALYSIS_READY,
    )
    _record(store, request)
    result = HuldraBroker(store=store, settings=settings).ensure(request)
    assert result.status == "ready"
    assert result.analysis_ready
    assert result.ready
    assert result.mature


def test_id_list_completed_cache_marks_maturity_not_applicable(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = ArxivRequest(
        client_id="demo",
        id_list=("2401.00001",),
        readiness=ReadinessMode.ANALYSIS_READY,
    )
    _record(store, request)
    result = HuldraBroker(store=store, settings=settings).ensure(request)
    assert result.status == "ready"
    assert not result.maturity_applicable
    assert result.mature


def test_analysis_ready_request_uses_caller_readiness_on_raw_cache_hit(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    raw_request = ArxivRequest(
        client_id="raw",
        search_query="cat:cs.AI",
        submitted_start=today,
        submitted_end=today + timedelta(days=1),
        readiness=ReadinessMode.RAW_COMPLETED,
    )
    analysis_request = raw_request.model_copy(
        update={
            "client_id": "analysis",
            "readiness": ReadinessMode.ANALYSIS_READY,
        }
    )
    _record(store, raw_request)

    result = HuldraBroker(store=store, settings=settings).ensure(analysis_request)

    assert result.status == "immature"
    assert not result.ready
    assert not result.analysis_ready
    assert result.maturity_applicable
    assert result.blocked_reason == "immature_window"


def test_raw_completed_request_can_use_analysis_ready_cache_and_reports_maturity_facts(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    analysis_request = ArxivRequest(
        client_id="analysis",
        search_query="cat:cs.AI",
        submitted_start=today,
        submitted_end=today + timedelta(days=1),
        readiness=ReadinessMode.ANALYSIS_READY,
    )
    raw_request = analysis_request.model_copy(
        update={
            "client_id": "raw",
            "readiness": ReadinessMode.RAW_COMPLETED,
        }
    )
    _record(store, analysis_request)

    result = HuldraBroker(store=store, settings=settings).ensure(raw_request)

    assert result.status == "ready"
    assert result.ready
    assert not result.analysis_ready
    assert result.maturity_applicable
    assert not result.mature
    assert result.blocked_reason == "immature_window"
    assert result.papers_total == 1


def test_maturity_lag_zero_disables_current_day_blocking(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    request = ArxivRequest(
        client_id="analysis",
        search_query="cat:cs.AI",
        submitted_start=today,
        submitted_end=today + timedelta(days=1),
        readiness=ReadinessMode.ANALYSIS_READY,
        maturity_lag_days=0,
    )
    _record(store, request)

    result = HuldraBroker(store=store, settings=settings).ensure(request)

    assert result.status == "ready"
    assert result.ready
    assert result.analysis_ready
    assert not result.maturity_applicable
    assert result.mature
    assert result.maturity_cutoff is None


def test_analysis_ready_stale_refresh_for_immature_window_suppresses_papers(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    request = ArxivRequest(
        client_id="analysis",
        search_query="cat:cs.AI",
        submitted_start=today,
        submitted_end=today + timedelta(days=1),
        readiness=ReadinessMode.ANALYSIS_READY,
        cache_policy=CachePolicy.STALE_WHILE_REVALIDATE,
    )
    _record(store, request)

    result = HuldraBroker(store=store, settings=settings).ensure(request)

    assert result.status == "immature"
    assert result.stale
    assert not result.ready
    assert not result.analysis_ready
    assert result.papers == []
    assert result.papers_total == 0
    assert result.cached_papers_total == 1
    assert result.request_id is not None
