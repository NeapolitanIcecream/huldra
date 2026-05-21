from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

from loguru import logger

from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.keys import request_cache_key
from huldra.models import (
    ArxivRequest,
    ArxivResult,
    BrokerStatus,
    CacheEntry,
    CachePolicy,
    ReadinessMode,
)
from huldra.time import ensure_utc, utc_now

log = logger.bind(module="huldra.broker")


class HuldraBroker:
    def __init__(
        self,
        store: HuldraStore | None = None,
        settings: HuldraSettings | None = None,
    ) -> None:
        self.settings = settings or HuldraSettings()
        self.store = store or HuldraStore(self.settings.db_path)
        self.store.init_schema()

    def ensure(self, request: ArxivRequest) -> ArxivResult:
        cache_key = request_cache_key(request)
        cached = self.store.get_cache_entry(cache_key)
        if cached and cached.status == "completed":
            result = self._result_from_completed_cache(cache_key, cached, cache_hit=True)
            if request.cache_policy == CachePolicy.STALE_WHILE_REVALIDATE:
                item = self.store.enqueue_request(request, cache_key)
                return result.model_copy(
                    update={
                        "status": "stale",
                        "stale": True,
                        "request_id": item.request_id,
                        "queued_at": item.created_at,
                    }
                )
            return result

        if request.cache_policy == CachePolicy.CACHE_ONLY:
            return ArxivResult(
                status="cache_miss",
                cache_key=cache_key,
                blocked_reason="cache_miss",
            )

        item = self.store.enqueue_request(request, cache_key)
        rate = self.store.get_rate_state()
        if rate.cooldown_until is not None and rate.cooldown_until > utc_now():
            return ArxivResult(
                status="cooling_down",
                cache_key=cache_key,
                request_id=item.request_id,
                queued_at=item.created_at,
                cooldown_until=rate.cooldown_until,
                blocked_reason="cooldown",
            )

        if request.cache_policy == CachePolicy.WAIT_UNTIL_READY:
            return self._wait_until_ready(request, cache_key, item.request_id)

        return ArxivResult(
            status="queued",
            cache_key=cache_key,
            request_id=item.request_id,
            queued_at=item.created_at,
        )

    def get_result(self, cache_key: str) -> ArxivResult:
        entry = self.store.get_cache_entry(cache_key)
        if entry is None:
            return ArxivResult(status="cache_miss", cache_key=cache_key)
        if entry.status == "completed":
            return self._result_from_completed_cache(cache_key, entry, cache_hit=True)
        status = "cooling_down" if entry.status == "rate_limited" else entry.status
        return ArxivResult(
            status=status,
            cache_key=cache_key,
            cooldown_until=entry.cooldown_until,
            blocked_reason=entry.error_category,
            error_category=entry.error_category,
            error_message=entry.error_message,
            upstream_status=entry.upstream_status,
        )

    def status(self) -> BrokerStatus:
        return self.store.status_summary()

    def _wait_until_ready(
        self,
        request: ArxivRequest,
        cache_key: str,
        request_id: str,
    ) -> ArxivResult:
        timeout = request.timeout_seconds or self.settings.request_timeout_seconds
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            entry = self.store.get_cache_entry(cache_key)
            if entry and entry.status == "completed":
                return self._result_from_completed_cache(
                    cache_key,
                    entry,
                    cache_hit=True,
                    request_id=request_id,
                )
            if entry and entry.status in {"failed", "rate_limited"}:
                return self.get_result(cache_key).model_copy(update={"request_id": request_id})
            time.sleep(min(0.1, max(0.01, timeout / 50)))
        log.bind(cache_key=cache_key, request_id=request_id).info("request_wait_timeout")
        return ArxivResult(
            status="timeout",
            cache_key=cache_key,
            request_id=request_id,
            blocked_reason="timeout",
        )

    def _result_from_completed_cache(
        self,
        cache_key: str,
        entry: CacheEntry,
        *,
        cache_hit: bool,
        request_id: str | None = None,
    ) -> ArxivResult:
        papers = self.store.get_cached_papers(cache_key)
        readiness = _evaluate_readiness(entry.request, self.settings)
        status = "ready" if readiness.analysis_ready else "immature"
        return ArxivResult(
            status=status,
            cache_key=cache_key,
            request_id=request_id,
            papers=papers,
            papers_total=len(papers),
            total_results=entry.total_results,
            cache_hit=cache_hit,
            ready=readiness.raw_completed,
            analysis_ready=readiness.analysis_ready,
            maturity_applicable=readiness.maturity_applicable,
            maturity_cutoff=readiness.maturity_cutoff,
            blocked_reason=readiness.blocked_reason,
            error_category=entry.error_category,
            error_message=entry.error_message,
            completed_at=entry.completed_at,
            cooldown_until=entry.cooldown_until,
            upstream_status=entry.upstream_status,
        )


class _Readiness:
    def __init__(
        self,
        *,
        raw_completed: bool,
        analysis_ready: bool,
        maturity_applicable: bool,
        maturity_cutoff: datetime | None,
        blocked_reason: str | None,
    ) -> None:
        self.raw_completed = raw_completed
        self.analysis_ready = analysis_ready
        self.maturity_applicable = maturity_applicable
        self.maturity_cutoff = maturity_cutoff
        self.blocked_reason = blocked_reason


def _evaluate_readiness(request: ArxivRequest, settings: HuldraSettings) -> _Readiness:
    if request.readiness == ReadinessMode.RAW_COMPLETED:
        return _Readiness(
            raw_completed=True,
            analysis_ready=True,
            maturity_applicable=False,
            maturity_cutoff=None,
            blocked_reason=None,
        )
    if request.submitted_start is None or request.submitted_end is None:
        return _Readiness(
            raw_completed=True,
            analysis_ready=True,
            maturity_applicable=False,
            maturity_cutoff=None,
            blocked_reason=None,
        )
    now = ensure_utc(utc_now())
    today = datetime(now.year, now.month, now.day, tzinfo=UTC)
    cutoff = today - timedelta(days=max(0, settings.maturity_lag_days - 1))
    mature = ensure_utc(request.submitted_end) <= cutoff
    return _Readiness(
        raw_completed=True,
        analysis_ready=mature,
        maturity_applicable=True,
        maturity_cutoff=cutoff,
        blocked_reason=None if mature else "immature_window",
    )
