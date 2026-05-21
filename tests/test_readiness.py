from __future__ import annotations

from datetime import UTC, datetime, timedelta

from huldra.broker import HuldraBroker
from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.keys import request_cache_key
from huldra.models import ArxivRequest, ReadinessMode
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
    assert result.ready
    assert not result.analysis_ready
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
