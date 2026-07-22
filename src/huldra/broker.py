from __future__ import annotations

import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from math import ceil
from uuid import uuid4

from loguru import logger

from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.fetcher import NonRetryableFetchError, RateLimitedError, TransientFetchError
from huldra.keys import normalize_arxiv_id, request_cache_key
from huldra.limiter import HuldraRateLimiter
from huldra.models import (
    ArxivRawInspectionResult,
    ArxivRequest,
    ArxivResult,
    BrokerStatus,
    CacheEntry,
    CachePolicy,
    CoverageStatus,
    HuldraMaintenanceRequestResult,
    HuldraMaintenanceResult,
    LegacySyncMode,
    OaiHarvestRequest,
    OaiHarvestResult,
    OaiRecord,
    QueueItem,
    QueueWorkKind,
    ReadinessMode,
)
from huldra.oai import OaiFetcher, OaiPmhFetcher, build_list_records_params
from huldra.time import ensure_utc, utc_now
from huldra.worker import Fetcher, HuldraWorker, WorkerPassResult

log = logger.bind(module="huldra.broker")

_PENDING_QUEUE_STATUSES = frozenset({"queued", "delayed", "claimed"})
_TERMINAL_DELAYED_ERROR_CATEGORIES = frozenset({"cooldown", "rate_limited"})
_INLINE_UPSTREAM_STATUSES = frozenset({"completed", "rate_limited", "transient_failure", "failed"})
_SYNC_JOB_TERMINAL_RAW_STATUSES = frozenset(
    {"completed", "failed", "rate_limited", "cache_unreadable", "skipped"}
)
_ASYNC_SYNC_JOB_HANDOFF_STATUSES = frozenset({"queued", "delayed", "claimed"})


@dataclass(slots=True)
class _MaintenanceTarget:
    request: ArxivRequest
    cache_key: str
    sync_job_id: str | None = None
    request_id: str | None = None
    joined_existing_queue: bool = False
    initial_cache_hit: bool = False
    coverage_status: CoverageStatus = CoverageStatus.UNKNOWN
    result_count: int = 0
    total_results: int | None = None
    pages_total: int = 0
    pages_completed_total: int = 0
    aggregate_raw_status: str | None = None
    aggregate_error_category: str | None = None
    aggregate_error_message: str | None = None


@dataclass(slots=True)
class _MaintenanceBudget:
    upstream_budget_id: str
    max_pages_per_window: int
    max_requests_total: int
    deadline: float
    deadline_at: datetime
    reserved_requests: int = 0


class HuldraBroker:
    def __init__(
        self,
        store: HuldraStore | None = None,
        settings: HuldraSettings | None = None,
        *,
        fetcher: Fetcher | None = None,
        oai_fetcher: OaiFetcher | None = None,
    ) -> None:
        self.settings = settings or HuldraSettings()
        self.store = store or HuldraStore(self.settings.db_path)
        self.fetcher = fetcher
        self.oai_fetcher = oai_fetcher
        self.store.init_schema()

    def ensure(self, request: ArxivRequest) -> ArxivResult:
        cache_key = request_cache_key(request)
        cached = self.store.get_readable_completed_cache(cache_key)
        if cached is not None:
            result = self._result_from_completed_cache(
                cache_key,
                cached,
                cache_hit=True,
                readiness_request=request,
            )
            if request.cache_policy == CachePolicy.STALE_WHILE_REVALIDATE:
                item, _joined = self.store.enqueue_refresh_if_due(
                    request,
                    cache_key,
                )
                if item is None:
                    return result
                reserved = self.store.get_cache_entry(cache_key)
                return result.model_copy(
                    update={
                        "stale": True,
                        "request_id": item.request_id,
                        "queued_at": item.created_at,
                        "refresh_after": (
                            reserved.refresh_after if reserved is not None else result.refresh_after
                        ),
                    }
                )
            return result

        composed = self._try_compose_cached_id_list(request, cache_key)
        if composed is not None:
            return composed

        if request.cache_policy == CachePolicy.CACHE_ONLY:
            return ArxivResult(
                serving_mode=request.readiness,
                status="cache_miss",
                cache_key=cache_key,
                blocked_reason="cache_miss",
            )

        item, _joined = self.store.enqueue_request_for_work(
            request,
            cache_key,
            work_kind=QueueWorkKind.FETCH_MISSING,
        )
        rate = self.store.get_rate_state()
        if rate.cooldown_until is not None and rate.cooldown_until > utc_now():
            return ArxivResult(
                serving_mode=request.readiness,
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
            serving_mode=request.readiness,
            status="queued",
            cache_key=cache_key,
            request_id=item.request_id,
            queued_at=item.created_at,
        )

    def get_result(self, cache_key: str) -> ArxivRawInspectionResult:
        entry = self.store.get_cache_entry(cache_key)
        if entry is None:
            return ArxivRawInspectionResult(status="cache_miss", cache_key=cache_key)
        if entry.status == "completed":
            readable = self.store.get_readable_completed_cache(cache_key)
            if readable is None:
                return ArxivRawInspectionResult(
                    status="cache_unreadable",
                    cache_key=cache_key,
                    cache_hit=True,
                    cache_readable=False,
                    total_results=entry.total_results,
                    coverage_status=entry.coverage_status,
                    error_category=entry.error_category,
                    error_message=entry.error_message,
                    completed_at=entry.completed_at,
                    cooldown_until=entry.cooldown_until,
                    upstream_status=entry.upstream_status,
                )
            papers = self.store.get_cached_papers(cache_key)
            return ArxivRawInspectionResult(
                status="ready",
                cache_key=cache_key,
                papers=papers,
                papers_total=len(papers),
                total_results=entry.total_results,
                coverage_status=entry.coverage_status,
                cache_hit=True,
                cache_readable=True,
                error_category=entry.error_category,
                error_message=entry.error_message,
                completed_at=entry.completed_at,
                cooldown_until=entry.cooldown_until,
                upstream_status=entry.upstream_status,
            )
        status = "cooling_down" if entry.status == "rate_limited" else entry.status
        return ArxivRawInspectionResult(
            status=status,
            cache_key=cache_key,
            cooldown_until=entry.cooldown_until,
            error_category=entry.error_category,
            error_message=entry.error_message,
            upstream_status=entry.upstream_status,
        )

    def status(self) -> BrokerStatus:
        return self.store.status_summary()

    def harvest_oai(self, request: OaiHarvestRequest) -> OaiHarvestResult:
        scope_lease_name = _oai_scope_lease_name(request)
        scope_owner = f"oai-harvest:{uuid4()}"
        scope_lease_seconds = max(
            self.settings.lease_timeout_seconds,
            ceil(
                self.settings.request_timeout_seconds
                + self.settings.request_interval_seconds
                + self.store.timeout
                + 10.0
            ),
        )
        if not self.store.acquire_lease(
            scope_lease_name,
            scope_owner,
            scope_lease_seconds,
        ):
            running = self.store.get_running_oai_harvest(request)
            if running is not None:
                active = self.store.get_oai_harvest_result(str(running["harvest_id"]))
                if active is not None:
                    return active.model_copy(
                        update={
                            "status": "blocked",
                            "error_category": "harvest_in_progress",
                            "error_message": "an OAI harvest for this scope is already running",
                        }
                    )
            return OaiHarvestResult(
                harvest_id=scope_lease_name,
                status="blocked",
                metadata_prefix=request.metadata_prefix,
                set_spec=request.set_spec,
                mode=request.mode,
                error_category="harvest_in_progress",
                error_message="an OAI harvest for this scope is already starting",
            )
        try:
            return self._harvest_oai_owned(
                request,
                scope_lease_name=scope_lease_name,
                scope_owner=scope_owner,
                scope_lease_seconds=scope_lease_seconds,
            )
        finally:
            self.store.release_lease(scope_lease_name, scope_owner)

    def _harvest_oai_owned(
        self,
        request: OaiHarvestRequest,
        *,
        scope_lease_name: str,
        scope_owner: str,
        scope_lease_seconds: int,
    ) -> OaiHarvestResult:
        running = self.store.get_running_oai_harvest(request)
        if running is None:
            resumption_token = (
                request.resumption_token
                or self.store.get_latest_resumable_oai_harvest_token(request)
            )
            harvest_id = self.store.create_oai_harvest_job(
                request,
                resumption_token=resumption_token,
            )
            effective_request = request
            page_index = 0
            requests_total = 0
            records_processed = 0
            papers_upserted = 0
            deleted_records = 0
            response_watermark = None
            max_datestamp_seen = None
            created = self.store.get_oai_harvest_result(harvest_id)
            assert created is not None and created.deadline_at is not None
            deadline_at = created.deadline_at
            finished_paging = False
            pending_error_category = None
            pending_error_message = None
        else:
            harvest_id = str(running["harvest_id"])
            effective_request = running["request"]
            assert isinstance(effective_request, OaiHarvestRequest)
            resumption_token = running["resumption_token"]
            page_index = int(running["pages_total"])
            requests_total = int(running["requests_total"])
            records_processed = int(running["records_processed"])
            papers_upserted = int(running["papers_upserted"])
            deleted_records = int(running["deleted_records"])
            response_watermark = running["last_response_date"]
            max_datestamp_seen = running["last_datestamp_seen"]
            deadline_at = running["deadline_at"]
            if deadline_at is None:
                deadline_at = self.store.ensure_oai_harvest_deadline(
                    harvest_id,
                    utc_now() + timedelta(seconds=effective_request.runtime_budget_seconds),
                )
            finished_paging = bool(running["finished_paging"])
            pending_error_category = running["error_category"]
            pending_error_message = running["error_message"]

        fetcher = self.oai_fetcher or OaiPmhFetcher(self.settings)
        # OAI-PMH and legacy Atom calls share arXiv's upstream throttle.
        # OAI-specific accounting stays in the harvest and page tables.
        limiter = HuldraRateLimiter(
            self.store,
            self.settings,
            lease_name="upstream_fetch",
        )
        owner_token = f"oai:{harvest_id}"
        from_datestamp = (
            None
            if resumption_token is not None
            else self._oai_start_datestamp(effective_request)
        )
        until_datestamp = _normalize_oai_day_datestamp(effective_request.until_datestamp)
        consumed_token_hashes = self.store.get_oai_consumed_token_hashes(harvest_id)

        def finish(
            *,
            status: str,
            pages_total: int | None = None,
            current_watermark: str | None = None,
            token: str | None = None,
            error_category: str | None = None,
            error_message: str | None = None,
        ) -> OaiHarvestResult:
            return self.store.complete_oai_harvest_job(
                harvest_id=harvest_id,
                status=status,
                records_processed=records_processed,
                papers_upserted=papers_upserted,
                deleted_records=deleted_records,
                pages_total=page_index if pages_total is None else pages_total,
                current_watermark=current_watermark,
                resumption_token=token,
                error_category=error_category,
                error_message=error_message,
            )

        def ownership_lost() -> OaiHarvestResult:
            # Never persist a terminal status after losing either lease: a
            # successor may already be checkpointing the same durable job.
            current = self.store.get_oai_harvest_result(harvest_id)
            if current is None:
                return OaiHarvestResult(
                    harvest_id=harvest_id,
                    status="blocked",
                    metadata_prefix=effective_request.metadata_prefix,
                    set_spec=effective_request.set_spec,
                    mode=effective_request.mode,
                    error_category="harvest_in_progress",
                    error_message="OAI harvest ownership changed before network I/O",
                )
            if current.status != "running":
                return current
            return current.model_copy(
                update={
                    "status": "blocked",
                    "error_category": "harvest_in_progress",
                    "error_message": "OAI harvest ownership changed before network I/O",
                }
            )

        if pending_error_category is not None:
            return finish(
                status="failed",
                current_watermark=response_watermark,
                token=resumption_token,
                error_category=str(pending_error_category),
                error_message=(
                    str(pending_error_message)
                    if pending_error_message is not None
                    else str(pending_error_category).replace("_", " ")
                ),
            )

        while not finished_paging:
            renewed = self.store.renew_lease_if_owned(
                scope_lease_name,
                scope_owner,
                scope_lease_seconds,
            )
            if not renewed:
                return ownership_lost()
            budget_error = _oai_budget_error(
                effective_request,
                pages_total=page_index,
                requests_total=requests_total,
                deadline_at=deadline_at,
            )
            if budget_error is not None:
                return finish(
                    status="budget_exceeded",
                    current_watermark=response_watermark,
                    token=resumption_token,
                    error_category=budget_error,
                    error_message=budget_error.replace("_", " "),
                )
            decision = limiter.before_request(owner_token=owner_token)
            request_params = build_list_records_params(
                metadata_prefix=effective_request.metadata_prefix,
                set_spec=effective_request.set_spec,
                from_datestamp=from_datestamp,
                until_datestamp=until_datestamp,
                resumption_token=resumption_token,
            )
            if not decision.can_fetch:
                status = "cooling_down" if decision.blocked_reason == "cooldown" else "blocked"
                self.store.record_oai_page(
                    harvest_id=harvest_id,
                    page_index=page_index,
                    request_params=request_params,
                    status=status,
                    resumption_token_hash=_hash_token(resumption_token),
                    error_category=decision.blocked_reason,
                    error_message=decision.blocked_reason,
                )
                return finish(
                    status=status,
                    pages_total=page_index + 1,
                    current_watermark=response_watermark,
                    token=resumption_token,
                    error_category=decision.blocked_reason,
                    error_message=decision.blocked_reason,
                )
            if (
                deadline_at is not None
                and utc_now() + timedelta(seconds=decision.wait_seconds) >= deadline_at
            ):
                self.store.release_lease("upstream_fetch", owner_token)
                return finish(
                    status="budget_exceeded",
                    current_watermark=response_watermark,
                    token=resumption_token,
                    error_category="runtime_budget_exceeded",
                    error_message="runtime budget would expire before the next request",
                )
            if decision.wait_seconds > 0:
                time.sleep(decision.wait_seconds)
            budget_error = _oai_budget_error(
                effective_request,
                pages_total=page_index,
                requests_total=requests_total,
                deadline_at=deadline_at,
            )
            if budget_error is not None:
                self.store.release_lease("upstream_fetch", owner_token)
                return finish(
                    status="budget_exceeded",
                    current_watermark=response_watermark,
                    token=resumption_token,
                    error_category=budget_error,
                    error_message=budget_error.replace("_", " "),
                )
            requests_total = self.store.record_oai_request_started(harvest_id)
            request_timeout_seconds = self.settings.request_timeout_seconds
            if deadline_at is not None:
                remaining_runtime_seconds = (deadline_at - utc_now()).total_seconds()
                if remaining_runtime_seconds <= 0:
                    self.store.release_lease("upstream_fetch", owner_token)
                    return finish(
                        status="budget_exceeded",
                        current_watermark=response_watermark,
                        token=resumption_token,
                        error_category="runtime_budget_exceeded",
                        error_message="runtime budget expired before the next request",
                    )
                request_timeout_seconds = min(
                    request_timeout_seconds,
                    remaining_runtime_seconds,
                )
            post_fetch_lease_seconds = max(
                self.settings.lease_timeout_seconds,
                ceil(
                    request_timeout_seconds
                    + self.store.timeout
                    + 5.0
                ),
            )
            if not self.store.renew_leases_if_owned(
                (
                    (scope_lease_name, scope_owner, scope_lease_seconds),
                    ("upstream_fetch", owner_token, post_fetch_lease_seconds),
                )
            ):
                self.store.release_lease("upstream_fetch", owner_token)
                return ownership_lost()
            if deadline_at is not None:
                remaining_runtime_seconds = (deadline_at - utc_now()).total_seconds()
                if remaining_runtime_seconds <= 0:
                    self.store.release_lease("upstream_fetch", owner_token)
                    return finish(
                        status="budget_exceeded",
                        current_watermark=response_watermark,
                        token=resumption_token,
                        error_category="runtime_budget_exceeded",
                        error_message="runtime budget expired before the next request",
                    )
                request_timeout_seconds = min(
                    request_timeout_seconds,
                    remaining_runtime_seconds,
                )
            try:
                page = fetcher.list_records(
                    metadata_prefix=effective_request.metadata_prefix,
                    set_spec=effective_request.set_spec,
                    from_datestamp=from_datestamp,
                    until_datestamp=until_datestamp,
                    resumption_token=resumption_token,
                    timeout_seconds=request_timeout_seconds,
                )
            except RateLimitedError as exc:
                cooldown_until = limiter.after_429(
                    owner_token=owner_token,
                    retry_after_seconds=exc.retry_after_seconds,
                    status_code=exc.status_code or 429,
                    error_message=str(exc),
                )
                self.store.record_oai_page(
                    harvest_id=harvest_id,
                    page_index=page_index,
                    request_params=request_params,
                    status="rate_limited",
                    resumption_token_hash=_hash_token(resumption_token),
                    error_category="rate_limited",
                    error_message=str(exc),
                )
                return finish(
                    status="rate_limited",
                    pages_total=page_index + 1,
                    current_watermark=response_watermark,
                    token=resumption_token,
                    error_category="rate_limited",
                    error_message=f"{exc}; cooldown_until={cooldown_until.isoformat()}",
                )
            except TransientFetchError as exc:
                limiter.after_failure(
                    owner_token=owner_token,
                    status=exc.status_code,
                    error_message=str(exc),
                )
                self.store.record_oai_page(
                    harvest_id=harvest_id,
                    page_index=page_index,
                    request_params=request_params,
                    status="transient_failure",
                    resumption_token_hash=_hash_token(resumption_token),
                    error_category="transient",
                    error_message=str(exc),
                )
                return finish(
                    status="transient_failure",
                    pages_total=page_index + 1,
                    current_watermark=response_watermark,
                    token=resumption_token,
                    error_category="transient",
                    error_message=str(exc),
                )
            except NonRetryableFetchError as exc:
                limiter.after_failure(
                    owner_token=owner_token,
                    status=exc.status_code,
                    error_message=str(exc),
                )
                self.store.record_oai_page(
                    harvest_id=harvest_id,
                    page_index=page_index,
                    request_params=request_params,
                    status="failed",
                    resumption_token_hash=_hash_token(resumption_token),
                    error_category="non_retryable",
                    error_message=str(exc),
                )
                return finish(
                    status="failed",
                    pages_total=page_index + 1,
                    current_watermark=response_watermark,
                    token=resumption_token,
                    error_category="non_retryable",
                    error_message=str(exc),
                )

            next_response_watermark = (
                _normalize_oai_day_datestamp(page.response_date, keep_invalid=False)
                or response_watermark
            )
            next_max_datestamp = _max_datestamp_seen(page.records, max_datestamp_seen)
            next_token = page.resumption_token
            next_token_hash = _hash_token(next_token)
            consumed_token_hash = _hash_token(resumption_token)
            token_error = _oai_token_error(
                records_total=len(page.records),
                consumed_token_hash=consumed_token_hash,
                next_token_hash=next_token_hash,
                consumed_token_hashes=consumed_token_hashes,
            )
            try:
                processed, upserted, deleted = self.store.checkpoint_oai_page(
                    harvest_id=harvest_id,
                    page_index=page_index,
                    request_params=page.request_params or request_params,
                    records=page.records,
                    response_date=page.response_date,
                    consumed_resumption_token=resumption_token,
                    consumed_resumption_token_hash=consumed_token_hash,
                    next_resumption_token=next_token,
                    next_resumption_token_hash=next_token_hash,
                    last_response_date=next_response_watermark,
                    last_datestamp_seen=next_max_datestamp,
                    terminal_error_category=token_error,
                    terminal_error_message=(
                        token_error.replace("_", " ") if token_error is not None else None
                    ),
                )
            except Exception as exc:
                limiter.after_failure(
                    owner_token=owner_token,
                    status=None,
                    error_message=f"OAI checkpoint failed: {exc}",
                )
                raise
            limiter.after_success(owner_token=owner_token)
            records_processed += processed
            papers_upserted += upserted
            deleted_records += deleted
            response_watermark = next_response_watermark
            max_datestamp_seen = next_max_datestamp
            page_index += 1
            if consumed_token_hash is not None:
                consumed_token_hashes.add(consumed_token_hash)
            resumption_token = next_token
            if token_error is not None:
                return finish(
                    status="failed",
                    current_watermark=response_watermark,
                    token=resumption_token,
                    error_category=token_error,
                    error_message=token_error.replace("_", " "),
                )
            finished_paging = next_token is None

        current_watermark = (
            response_watermark
            if _should_commit_oai_watermark(effective_request)
            else max_datestamp_seen
        ) or max_datestamp_seen or from_datestamp
        if _should_commit_oai_watermark(effective_request):
            self.store.set_oai_watermark(
                metadata_prefix=effective_request.metadata_prefix,
                set_spec=effective_request.set_spec,
                last_response_date=response_watermark,
                last_datestamp_seen=max_datestamp_seen,
                harvest_id=harvest_id,
            )
        return finish(
            status="completed",
            current_watermark=current_watermark,
        )

    def _oai_start_datestamp(self, request: OaiHarvestRequest) -> str | None:
        if request.from_datestamp:
            return _normalize_oai_day_datestamp(request.from_datestamp)
        if request.mode != "incremental":
            return None
        watermark = self.store.get_oai_watermark(
            metadata_prefix=request.metadata_prefix,
            set_spec=request.set_spec,
        )
        if watermark is None:
            return None
        for raw in (watermark.get("last_response_date"), watermark.get("last_datestamp_seen")):
            normalized = _normalize_oai_day_datestamp(raw, keep_invalid=False)
            if normalized is None:
                continue
            if self.settings.oai_overlap_seconds <= 0:
                return normalized
            return _subtract_oai_overlap(normalized, self.settings.oai_overlap_seconds)
        return None

    def sync_windows(
        self,
        requests: list[ArxivRequest],
        *,
        wait: bool = False,
        wait_timeout_seconds: float | None = None,
        mode: LegacySyncMode = LegacySyncMode.SLICE,
        max_pages_per_window: int = 100,
        max_requests_total: int = 500,
    ) -> HuldraMaintenanceResult:
        if mode == LegacySyncMode.COMPLETE_WINDOW and not wait:
            raise ValueError("complete_window mode requires wait=True")
        if max_pages_per_window <= 0 or max_requests_total <= 0:
            raise ValueError("maintenance budgets must be positive")
        if not requests:
            return HuldraMaintenanceResult()
        initial_plan = [
            (
                request,
                request_cache_key(request),
                self.store.get_readable_completed_cache(request_cache_key(request)),
            )
            for request in requests
        ]
        initial_requests_total = len(
            {
                cache_key
                for _request, cache_key, cached in initial_plan
                if cached is None
            }
        )
        if initial_requests_total > max_requests_total:
            raise ValueError(
                f"maintenance plan exceeds request budget: "
                f"required={initial_requests_total}, limit={max_requests_total}"
            )
        timeout = _maintenance_timeout_seconds(requests, self.settings, wait_timeout_seconds)
        deadline = time.monotonic() + timeout
        deadline_at = utc_now() + timedelta(seconds=timeout)
        if (
            mode == LegacySyncMode.COMPLETE_WINDOW
            and initial_requests_total > 0
            and not _deadline_can_fit_requests(
                deadline,
                initial_requests_total,
                self.settings.request_interval_seconds,
                first_request_may_start_now=True,
            )
        ):
            raise ValueError("maintenance plan exceeds runtime deadline budget")
        upstream_budget_id = self.store.create_upstream_request_budget(
            max_requests=max_requests_total,
            deadline_at=(deadline_at if mode == LegacySyncMode.COMPLETE_WINDOW else None),
        )
        budget = _MaintenanceBudget(
            upstream_budget_id=upstream_budget_id,
            max_pages_per_window=max_pages_per_window,
            max_requests_total=max_requests_total,
            deadline=deadline,
            deadline_at=deadline_at,
            reserved_requests=initial_requests_total,
        )
        targets = []
        initial_cache: dict[str, CacheEntry | None] = {}
        for request, cache_key, readable in initial_plan:
            initial_cache[cache_key] = readable
            sync_job_id = self.store.create_sync_job(
                request,
                mode,
                upstream_budget_id=budget.upstream_budget_id,
            )
            self.store.record_sync_job_page(
                sync_job_id=sync_job_id,
                request=request,
                cache_key=cache_key,
                status="planned",
            )
            targets.append(
                _MaintenanceTarget(
                    request=request,
                    cache_key=cache_key,
                    sync_job_id=sync_job_id,
                    pages_total=1,
                )
            )
        result = HuldraMaintenanceResult(requested_total=len(targets))
        for target in targets:
            readable = initial_cache[target.cache_key]
            if readable is not None:
                target.initial_cache_hit = True
                self.store.refresh_sync_job_page_from_cache(
                    sync_job_id=target.sync_job_id or "",
                    request=target.request,
                    cache_key=target.cache_key,
                )
                result.cache_hit_total += 1
                continue
            result.cache_miss_total += 1
            if (
                mode == LegacySyncMode.COMPLETE_WINDOW
                and time.monotonic() >= budget.deadline
            ):
                self.store.fail_active_upstream_budget_items(
                    budget.upstream_budget_id,
                    error_category="deadline_budget_exceeded",
                    error_message="maintenance deadline expired before initial enqueue",
                )
                self._complete_initial_maintenance_budget_failure(
                    target,
                    error_category="deadline_budget_exceeded",
                    error_message="maintenance deadline expired before initial enqueue",
                )
                continue
            queue_request = target.request.model_copy(update={"cache_policy": CachePolicy.CACHE_OR_ENQUEUE})
            item, joined = self.store.enqueue_request_for_work(
                queue_request,
                target.cache_key,
                work_kind=QueueWorkKind.FETCH_MISSING,
                upstream_budget_id=budget.upstream_budget_id,
            )
            target.request_id = item.request_id
            target.joined_existing_queue = joined
            if target.sync_job_id is not None:
                self.store.record_sync_job_page(
                    sync_job_id=target.sync_job_id,
                    request=target.request,
                    cache_key=target.cache_key,
                    status=_raw_status_from_queue_item(item) or str(item.status),
                )
            result.queued_total += 1

        if wait and targets:
            if mode == LegacySyncMode.COMPLETE_WINDOW:
                result = self._drain_complete_window_targets(
                    targets,
                    result,
                    budget,
                    timeout,
                )
            else:
                result = result.model_copy(
                    update=self._drain_maintenance_targets(
                        targets,
                        result,
                        wait_timeout_seconds,
                    ).model_dump()
                )

        return self._finalize_maintenance_result(
            targets,
            result,
            mode=mode,
            complete_pending_jobs=not wait,
        )

    def backfill_windows(
        self,
        *,
        search_queries: list[str],
        start_date: date,
        end_date: date,
        max_results: int,
        wait: bool = False,
        wait_timeout_seconds: float | None = None,
        mode: LegacySyncMode = LegacySyncMode.SLICE,
        client_id: str = "huldra-backfill",
        max_pages_per_window: int = 100,
        max_requests_total: int = 500,
    ) -> HuldraMaintenanceResult:
        from huldra.planner import build_submitted_date_windows

        return self.sync_windows(
            build_submitted_date_windows(
                search_queries=search_queries,
                start_date=start_date,
                end_date=end_date,
                max_results=max_results,
                client_id=client_id,
            ),
            wait=wait,
            wait_timeout_seconds=wait_timeout_seconds,
            mode=mode,
            max_pages_per_window=max_pages_per_window,
            max_requests_total=max_requests_total,
        )

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
                readable = self.store.get_readable_completed_cache(cache_key)
                if readable is not None:
                    return self._result_from_completed_cache(
                        cache_key,
                        readable,
                        cache_hit=True,
                        request_id=request_id,
                        readiness_request=request,
                    )
                item = self.store.get_queue_item(request_id)
                if item is None or _queue_item_has_terminal_outcome(item):
                    raw = self.get_result(cache_key)
                    return self._result_from_terminal_raw_state(
                        request,
                        raw,
                        request_id=request_id,
                        queue_item=item,
                    )
            if entry and entry.status in {"failed", "rate_limited"}:
                item = self.store.get_queue_item(request_id)
                if item is None or _queue_item_has_terminal_outcome(item):
                    raw = self.get_result(cache_key)
                    return self._result_from_terminal_raw_state(
                        request,
                        raw,
                        request_id=request_id,
                        queue_item=item,
                    )
            time.sleep(min(0.1, max(0.01, timeout / 50)))
        log.bind(cache_key=cache_key, request_id=request_id).info("request_wait_timeout")
        return ArxivResult(
            serving_mode=request.readiness,
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
        readiness_request: ArxivRequest | None = None,
    ) -> ArxivResult:
        papers = self.store.get_cached_papers(cache_key)
        request = readiness_request or entry.request
        readiness = _evaluate_readiness(request, self.settings)
        accepted = request.readiness == ReadinessMode.RAW_COMPLETED or readiness.analysis_ready
        status = "ready" if accepted else "immature"
        exposed_papers = papers if accepted else []
        return ArxivResult(
            serving_mode=request.readiness,
            status=status,
            cache_key=cache_key,
            request_id=request_id,
            papers=exposed_papers,
            papers_total=len(exposed_papers),
            cached_papers_total=len(papers),
            total_results=entry.total_results,
            coverage_status=entry.coverage_status,
            cache_hit=cache_hit,
            cache_readable=True,
            mature=readiness.mature,
            ready=accepted,
            analysis_ready=readiness.analysis_ready,
            maturity_applicable=readiness.maturity_applicable,
            maturity_cutoff=readiness.maturity_cutoff,
            blocked_reason=readiness.blocked_reason,
            error_category=entry.error_category,
            error_message=entry.error_message,
            completed_at=entry.completed_at,
            refresh_after=entry.refresh_after,
            cooldown_until=entry.cooldown_until,
            upstream_status=entry.upstream_status,
        )

    def _result_from_terminal_raw_state(
        self,
        request: ArxivRequest,
        raw: ArxivRawInspectionResult,
        *,
        request_id: str,
        queue_item: QueueItem | None,
    ) -> ArxivResult:
        status = raw.status
        cooldown_until = raw.cooldown_until
        blocked_reason = raw.blocked_reason
        error_category = raw.error_category
        error_message = raw.error_message
        if queue_item is not None:
            if queue_item.status == "failed":
                status = "failed"
            elif (
                queue_item.status == "delayed"
                and queue_item.error_category in _TERMINAL_DELAYED_ERROR_CATEGORIES
            ):
                status = "cooling_down"
                blocked_reason = queue_item.error_category
            cooldown_until = queue_item.next_attempt_at or cooldown_until
            error_category = queue_item.error_category or error_category
            error_message = queue_item.error_message or error_message
        return ArxivResult(
            serving_mode=request.readiness,
            status=status,
            cache_key=raw.cache_key,
            request_id=request_id,
            papers=raw.papers,
            papers_total=raw.papers_total,
            total_results=raw.total_results,
            coverage_status=raw.coverage_status,
            cache_hit=raw.cache_hit,
            cache_readable=raw.cache_readable,
            cooldown_until=cooldown_until,
            blocked_reason=blocked_reason,
            error_category=error_category,
            error_message=error_message,
            completed_at=raw.completed_at,
            upstream_status=raw.upstream_status,
        )

    def _try_compose_cached_id_list(self, request: ArxivRequest, cache_key: str) -> ArxivResult | None:
        if not _is_pure_id_list_request(request):
            return None
        ids = tuple(dict.fromkeys(normalize_arxiv_id(value) for value in request.id_list))
        papers_by_id = self.store.get_papers_by_ids(ids)
        if any(arxiv_id not in papers_by_id for arxiv_id in ids):
            return None
        papers = [papers_by_id[arxiv_id] for arxiv_id in ids]
        self.store.record_completed_cache_entry(
            cache_key=cache_key,
            request=request,
            papers=papers,
            total_results=len(papers),
            upstream_request_count=0,
        )
        entry = self.store.get_readable_completed_cache(cache_key)
        assert entry is not None
        return self._result_from_completed_cache(cache_key, entry, cache_hit=True, readiness_request=request)

    def _drain_maintenance_targets(
        self,
        targets: list[_MaintenanceTarget],
        result: HuldraMaintenanceResult,
        wait_timeout_seconds: float | None,
    ) -> HuldraMaintenanceResult:
        timeout = _maintenance_timeout_seconds(
            [target.request for target in targets],
            self.settings,
            wait_timeout_seconds,
        )
        deadline = time.monotonic() + timeout
        target_keys = frozenset(target.cache_key for target in targets)
        while time.monotonic() < deadline:
            if all(self._target_terminal(target) for target in targets):
                break
            worker_result = HuldraWorker(
                self.store,
                self.settings,
                fetcher=self.fetcher,
            ).run_once(target_cache_keys=target_keys)
            result = _count_inline_worker_result(result, worker_result)
            if worker_result.status == "idle":
                time.sleep(min(0.05, max(0.01, timeout / 50)))
        return result

    def _drain_complete_window_targets(
        self,
        targets: list[_MaintenanceTarget],
        result: HuldraMaintenanceResult,
        budget: _MaintenanceBudget,
        timeout: float,
    ) -> HuldraMaintenanceResult:
        result = self._drain_targets_until_deadline(
            targets,
            result,
            budget.deadline,
            timeout,
            upstream_budget_id=budget.upstream_budget_id,
        )
        for target in targets:
            result = self._plan_and_drain_complete_window_target(
                target,
                result,
                budget,
                timeout,
            )
        return result

    def _drain_targets_until_deadline(
        self,
        targets: list[_MaintenanceTarget],
        result: HuldraMaintenanceResult,
        deadline: float,
        timeout: float,
        *,
        upstream_budget_id: str | None = None,
    ) -> HuldraMaintenanceResult:
        if not targets:
            return result
        target_keys = frozenset(target.cache_key for target in targets)
        while time.monotonic() < deadline:
            if all(self._target_terminal(target) for target in targets):
                break
            worker_result = HuldraWorker(
                self.store,
                self.settings,
                fetcher=self.fetcher,
            ).run_once(target_cache_keys=target_keys)
            result = _count_inline_worker_result(result, worker_result)
            if worker_result.status == "idle":
                time.sleep(min(0.05, max(0.01, timeout / 50)))
        if upstream_budget_id is not None and time.monotonic() >= deadline:
            self.store.fail_active_upstream_budget_items(
                upstream_budget_id,
                error_category="deadline_budget_exceeded",
                error_message="maintenance deadline expired before upstream fetch",
            )
        return result

    def _plan_and_drain_complete_window_target(
        self,
        target: _MaintenanceTarget,
        result: HuldraMaintenanceResult,
        budget: _MaintenanceBudget,
        timeout: float,
    ) -> HuldraMaintenanceResult:
        sync_job_id = target.sync_job_id
        assert sync_job_id is not None
        if (
            target.request_id is None
            and target.aggregate_error_category == "deadline_budget_exceeded"
        ):
            return result
        first = self.store.get_readable_completed_cache(target.cache_key)
        self.store.refresh_sync_job_page_from_cache(
            sync_job_id=sync_job_id,
            request=target.request,
            cache_key=target.cache_key,
        )
        if first is None:
            entry = self.store.get_cache_entry(target.cache_key)
            queue_item = (
                self.store.get_queue_item(target.request_id)
                if target.request_id is not None
                else None
            )
            target.coverage_status = CoverageStatus.PARTIAL
            target.aggregate_raw_status = (
                entry.status
                if entry is not None
                else (str(queue_item.status) if queue_item is not None else "partial")
            )
            target.aggregate_error_category = (
                entry.error_category
                if entry is not None
                else (queue_item.error_category if queue_item is not None else None)
            )
            target.aggregate_error_message = (
                entry.error_message
                if entry is not None
                else (queue_item.error_message if queue_item is not None else None)
            )
            target.pages_total = 1
            target.pages_completed_total = 0
            self.store.complete_sync_job(
                sync_job_id=sync_job_id,
                status="partial",
                coverage_status=CoverageStatus.PARTIAL,
                result_count=0,
                total_results=entry.total_results if entry is not None else None,
                pages_total=1,
                pages_completed_total=0,
                error_category=target.aggregate_error_category,
                error_message=target.aggregate_error_message,
            )
            return result

        target.total_results = first.total_results
        if first.total_results is None:
            target.coverage_status = CoverageStatus.PARTIAL
            target.result_count = first.result_count
            target.pages_total = 1
            target.pages_completed_total = 1
            target.aggregate_raw_status = "partial"
            self.store.complete_sync_job(
                sync_job_id=sync_job_id,
                status="partial",
                coverage_status=CoverageStatus.PARTIAL,
                result_count=first.result_count,
                total_results=None,
                pages_total=1,
                pages_completed_total=1,
                error_category="missing_total_results",
                error_message="legacy search response did not include totalResults",
            )
            return result

        if first.total_results > self.settings.legacy_search_window_result_cap:
            target.coverage_status = CoverageStatus.OVERFLOW
            target.result_count = first.result_count
            target.pages_total = 1
            target.pages_completed_total = 1
            target.aggregate_raw_status = "overflow"
            target.aggregate_error_category = "legacy_window_overflow"
            target.aggregate_error_message = (
                "legacy search window total_results exceeds configured cap"
            )
            self.store.complete_sync_job(
                sync_job_id=sync_job_id,
                status="overflow",
                coverage_status=CoverageStatus.OVERFLOW,
                result_count=first.result_count,
                total_results=first.total_results,
                pages_total=1,
                pages_completed_total=1,
                error_category=target.aggregate_error_category,
                error_message=target.aggregate_error_message,
            )
            return result

        if first.result_count <= 0 and target.request.start < first.total_results:
            target.coverage_status = CoverageStatus.PARTIAL
            target.result_count = first.result_count
            target.pages_total = 1
            target.pages_completed_total = 1
            target.aggregate_raw_status = "partial"
            target.aggregate_error_category = "empty_page"
            target.aggregate_error_message = "legacy search first page did not advance coverage"
            self.store.complete_sync_job(
                sync_job_id=sync_job_id,
                status="partial",
                coverage_status=CoverageStatus.PARTIAL,
                result_count=first.result_count,
                total_results=first.total_results,
                pages_total=1,
                pages_completed_total=1,
                error_category=target.aggregate_error_category,
                error_message=target.aggregate_error_message,
            )
            return result

        next_start = target.request.start + first.result_count
        remaining_results = max(0, first.total_results - next_start)
        followup_pages_total = (
            (remaining_results + target.request.max_results - 1) // target.request.max_results
        )
        required_pages_total = 1 + followup_pages_total
        if required_pages_total > budget.max_pages_per_window:
            self._complete_window_budget_failure(
                target,
                first,
                error_category="page_budget_exceeded",
                error_message=(
                    f"complete window requires {required_pages_total} pages; "
                    f"limit is {budget.max_pages_per_window}"
                ),
            )
            return result

        planned_pages: list[tuple[ArxivRequest, str, CacheEntry | None]] = []
        while next_start < first.total_results:
            page_request = target.request.model_copy(
                update={
                    "start": next_start,
                    "cache_policy": CachePolicy.CACHE_OR_ENQUEUE,
                }
            )
            page_key = request_cache_key(page_request)
            planned_pages.append(
                (
                    page_request,
                    page_key,
                    self.store.get_readable_completed_cache(page_key),
                )
            )
            next_start += target.request.max_results

        missing_pages_total = sum(1 for _request, _key, cached in planned_pages if cached is None)
        if budget.reserved_requests + missing_pages_total > budget.max_requests_total:
            self._complete_window_budget_failure(
                target,
                first,
                error_category="request_budget_exceeded",
                error_message=(
                    f"complete window would reserve {missing_pages_total} additional requests; "
                    f"{budget.reserved_requests} of {budget.max_requests_total} already reserved"
                ),
            )
            return result
        if missing_pages_total and not _deadline_can_fit_requests(
            budget.deadline,
            missing_pages_total,
            self.settings.request_interval_seconds,
            first_request_may_start_now=False,
        ):
            self._complete_window_budget_failure(
                target,
                first,
                error_category="deadline_budget_exceeded",
                error_message="complete window cannot finish before the maintenance deadline",
            )
            return result
        budget.reserved_requests += missing_pages_total

        page_targets = [target]
        for page_request, page_key, cached in planned_pages:
            if cached is None and time.monotonic() >= budget.deadline:
                self._complete_followup_deadline_budget_failure(target, first, budget)
                return result
            page_target = _MaintenanceTarget(
                request=page_request,
                cache_key=page_key,
                sync_job_id=sync_job_id,
                pages_total=1,
            )
            self.store.record_sync_job_page(
                sync_job_id=sync_job_id,
                request=page_request,
                cache_key=page_key,
                status="planned",
            )
            if cached is None:
                if time.monotonic() >= budget.deadline:
                    self._complete_followup_deadline_budget_failure(target, first, budget)
                    return result
                item, joined = self.store.enqueue_request_for_work(
                    page_request,
                    page_key,
                    work_kind=QueueWorkKind.FETCH_MISSING,
                    upstream_budget_id=budget.upstream_budget_id,
                )
                page_target.request_id = item.request_id
                page_target.joined_existing_queue = joined
                if not joined:
                    result = result.model_copy(update={"queued_total": result.queued_total + 1})
                if time.monotonic() >= budget.deadline:
                    self._complete_followup_deadline_budget_failure(target, first, budget)
                    return result
            else:
                page_target.initial_cache_hit = True
            page_targets.append(page_target)

        result = self._drain_targets_until_deadline(
            page_targets[1:],
            result,
            budget.deadline,
            timeout,
            upstream_budget_id=budget.upstream_budget_id,
        )
        self._finalize_complete_window_target(target, page_targets)
        return result

    def _complete_followup_deadline_budget_failure(
        self,
        target: _MaintenanceTarget,
        first: CacheEntry,
        budget: _MaintenanceBudget,
    ) -> None:
        sync_job_id = target.sync_job_id
        assert sync_job_id is not None
        error_message = "maintenance deadline expired before follow-up enqueue"
        self.store.fail_active_upstream_budget_items(
            budget.upstream_budget_id,
            error_category="deadline_budget_exceeded",
            error_message=error_message,
        )
        self.store.delete_sync_job_followup_pages(
            sync_job_id=sync_job_id,
            initial_cache_key=target.cache_key,
        )
        self._complete_window_budget_failure(
            target,
            first,
            error_category="deadline_budget_exceeded",
            error_message=error_message,
        )

    def _complete_initial_maintenance_budget_failure(
        self,
        target: _MaintenanceTarget,
        *,
        error_category: str,
        error_message: str,
    ) -> None:
        sync_job_id = target.sync_job_id
        assert sync_job_id is not None
        target.coverage_status = CoverageStatus.PARTIAL
        target.pages_total = 1
        target.pages_completed_total = 0
        target.aggregate_raw_status = "partial"
        target.aggregate_error_category = error_category
        target.aggregate_error_message = error_message
        self.store.complete_sync_job(
            sync_job_id=sync_job_id,
            status="partial",
            coverage_status=CoverageStatus.PARTIAL,
            result_count=0,
            total_results=None,
            pages_total=1,
            pages_completed_total=0,
            error_category=error_category,
            error_message=error_message,
        )

    def _complete_window_budget_failure(
        self,
        target: _MaintenanceTarget,
        first: CacheEntry,
        *,
        error_category: str,
        error_message: str,
    ) -> None:
        sync_job_id = target.sync_job_id
        assert sync_job_id is not None
        target.coverage_status = CoverageStatus.PARTIAL
        target.result_count = first.result_count
        target.total_results = first.total_results
        target.pages_total = 1
        target.pages_completed_total = 1
        target.aggregate_raw_status = "partial"
        target.aggregate_error_category = error_category
        target.aggregate_error_message = error_message
        self.store.complete_sync_job(
            sync_job_id=sync_job_id,
            status="partial",
            coverage_status=CoverageStatus.PARTIAL,
            result_count=first.result_count,
            total_results=first.total_results,
            pages_total=1,
            pages_completed_total=1,
            error_category=error_category,
            error_message=error_message,
        )

    def _finalize_complete_window_target(
        self,
        target: _MaintenanceTarget,
        page_targets: list[_MaintenanceTarget],
    ) -> None:
        sync_job_id = target.sync_job_id
        assert sync_job_id is not None
        result_count = 0
        total_results = target.total_results
        pages_completed = 0
        first_error_category = None
        first_error_message = None
        for page_target in page_targets:
            page = self.store.refresh_sync_job_page_from_cache(
                sync_job_id=sync_job_id,
                request=page_target.request,
                cache_key=page_target.cache_key,
            )
            readable = self.store.get_readable_completed_cache(page_target.cache_key)
            if readable is not None:
                result_count += readable.result_count
                total_results = readable.total_results if total_results is None else total_results
                pages_completed += 1
            elif first_error_category is None:
                first_error_category = page.get("error_category")
                entry = self.store.get_cache_entry(page_target.cache_key)
                first_error_message = entry.error_message if entry is not None else None
        pages_total = len(page_targets)
        complete = (
            total_results is not None
            and pages_completed == pages_total
            and result_count >= max(0, total_results - target.request.start)
        )
        target.coverage_status = CoverageStatus.COMPLETE if complete else CoverageStatus.PARTIAL
        target.result_count = result_count
        target.total_results = total_results
        target.pages_total = pages_total
        target.pages_completed_total = pages_completed
        target.aggregate_raw_status = "completed" if complete else "partial"
        target.aggregate_error_category = None if complete else first_error_category
        target.aggregate_error_message = None if complete else first_error_message
        self.store.complete_sync_job(
            sync_job_id=sync_job_id,
            status="completed" if complete else "partial",
            coverage_status=target.coverage_status,
            result_count=result_count,
            total_results=total_results,
            pages_total=pages_total,
            pages_completed_total=pages_completed,
            error_category=target.aggregate_error_category,
            error_message=target.aggregate_error_message,
        )

    def _target_terminal(self, target: _MaintenanceTarget) -> bool:
        if self.store.get_readable_completed_cache(target.cache_key) is not None:
            return True
        entry = self.store.get_cache_entry(target.cache_key)
        if entry is not None and entry.status in {"failed", "rate_limited"}:
            return True
        if target.request_id is None:
            return False
        item = self.store.get_queue_item(target.request_id)
        return _queue_item_has_terminal_outcome(item)

    def _finalize_maintenance_result(
        self,
        targets: list[_MaintenanceTarget],
        result: HuldraMaintenanceResult,
        *,
        mode: LegacySyncMode,
        complete_pending_jobs: bool = False,
    ) -> HuldraMaintenanceResult:
        entries: list[HuldraMaintenanceRequestResult] = []
        completed_total = 0
        completed_slices_total = 0
        complete_windows_total = 0
        partial_windows_total = 0
        overflow_windows_total = 0
        papers_total = 0
        cooldown_active_total = 0
        skipped_total = 0
        rate_limited_total = 0
        failed_total = 0
        budget_exhausted_total = 0
        cooldown_until = None
        for target in targets:
            readable = self.store.get_readable_completed_cache(target.cache_key)
            cache_entry = self.store.get_cache_entry(target.cache_key)
            queue_item = (
                self.store.get_queue_item(target.request_id) if target.request_id is not None else None
            )
            serving = (
                self._result_from_completed_cache(
                    target.cache_key,
                    readable,
                    cache_hit=target.initial_cache_hit,
                    request_id=target.request_id,
                    readiness_request=target.request,
                )
                if readable is not None
                else None
            )
            if mode == LegacySyncMode.COMPLETE_WINDOW and target.coverage_status != CoverageStatus.UNKNOWN:
                raw_status = target.aggregate_raw_status or str(target.coverage_status)
                if target.coverage_status == CoverageStatus.COMPLETE:
                    completed_total += 1
                    complete_windows_total += 1
                    papers_total += target.result_count
                elif target.coverage_status == CoverageStatus.PARTIAL:
                    partial_windows_total += 1
                elif target.coverage_status == CoverageStatus.OVERFLOW:
                    overflow_windows_total += 1
                completed_slices_total += target.pages_completed_total
            elif readable is not None:
                raw_status = "completed"
                if target.sync_job_id is not None:
                    self.store.refresh_sync_job_page_from_cache(
                        sync_job_id=target.sync_job_id,
                        request=target.request,
                        cache_key=target.cache_key,
                    )
                completed_total += 1
                completed_slices_total += 1
                papers_total += readable.result_count
                target.coverage_status = readable.coverage_status
                target.result_count = readable.result_count
                target.total_results = readable.total_results
                target.pages_total = max(target.pages_total, 1)
                target.pages_completed_total = 1
            elif cache_entry is None:
                raw_status = _raw_status_from_queue_item(queue_item) or "missing"
            elif cache_entry.status == "completed":
                raw_status = _unreadable_completed_raw_status(queue_item)
                if target.sync_job_id is not None:
                    self.store.refresh_sync_job_page_from_cache(
                        sync_job_id=target.sync_job_id,
                        request=target.request,
                        cache_key=target.cache_key,
                    )
            else:
                raw_status = cache_entry.status
                if target.sync_job_id is not None:
                    self.store.refresh_sync_job_page_from_cache(
                        sync_job_id=target.sync_job_id,
                        request=target.request,
                        cache_key=target.cache_key,
                    )
            if readable is None:
                if raw_status == "skipped":
                    skipped_total += 1
                elif raw_status == "rate_limited":
                    rate_limited_total += 1
                elif raw_status in {"failed", "cache_unreadable", "partial", "overflow"}:
                    failed_total += 1
            if cache_entry is not None and cache_entry.cooldown_until is not None:
                cooldown_until = cache_entry.cooldown_until
                if cache_entry.cooldown_until > utc_now():
                    cooldown_active_total += 1
            elif queue_item is not None and queue_item.error_category in _TERMINAL_DELAYED_ERROR_CATEGORIES:
                cooldown_until = queue_item.next_attempt_at
                if queue_item.next_attempt_at is not None and queue_item.next_attempt_at > utc_now():
                    cooldown_active_total += 1
            request_cooldown_until = cache_entry.cooldown_until if cache_entry is not None else None
            request_error_category = cache_entry.error_category if cache_entry is not None else None
            request_error_message = cache_entry.error_message if cache_entry is not None else None
            if target.aggregate_error_category is not None:
                request_error_category = target.aggregate_error_category
            if target.aggregate_error_message is not None:
                request_error_message = target.aggregate_error_message
            if request_error_category in {
                "page_budget_exceeded",
                "request_budget_exceeded",
                "deadline_budget_exceeded",
            }:
                budget_exhausted_total += 1
            if queue_item is not None and (cache_entry is None or cache_entry.status == "completed"):
                request_cooldown_until = queue_item.next_attempt_at or request_cooldown_until
                request_error_category = queue_item.error_category or request_error_category
                request_error_message = queue_item.error_message or request_error_message
            if (
                target.sync_job_id is not None
                and mode == LegacySyncMode.SLICE
                and (
                    raw_status in _SYNC_JOB_TERMINAL_RAW_STATUSES
                    or (
                        complete_pending_jobs
                        and raw_status in _ASYNC_SYNC_JOB_HANDOFF_STATUSES
                    )
                )
            ):
                self.store.complete_sync_job(
                    sync_job_id=target.sync_job_id,
                    status=raw_status,
                    coverage_status=target.coverage_status,
                    result_count=target.result_count,
                    total_results=target.total_results,
                    pages_total=max(target.pages_total, 1),
                    pages_completed_total=target.pages_completed_total,
                    error_category=request_error_category,
                    error_message=request_error_message,
                )
            entries.append(
                HuldraMaintenanceRequestResult(
                    sync_job_id=target.sync_job_id,
                    cache_key=target.cache_key,
                    request_id=target.request_id,
                    search_query=target.request.search_query,
                    submitted_start=target.request.submitted_start,
                    submitted_end=target.request.submitted_end,
                    raw_cache_status=raw_status,
                    serving_status=serving.status if serving is not None else raw_status,
                    coverage_status=target.coverage_status,
                    cache_hit=target.initial_cache_hit,
                    joined_existing_queue=target.joined_existing_queue,
                    upstream_status=cache_entry.upstream_status if cache_entry is not None else None,
                    cooldown_until=request_cooldown_until,
                    error_category=request_error_category,
                    error_message=request_error_message,
                    papers_total=target.result_count,
                    result_count=target.result_count,
                    total_results=target.total_results,
                    pages_total=target.pages_total,
                    pages_completed_total=target.pages_completed_total,
                )
            )
        return result.model_copy(
            update={
                "completed_windows_total": completed_total,
                "completed_slices_total": completed_slices_total,
                "complete_windows_total": complete_windows_total,
                "partial_windows_total": partial_windows_total,
                "overflow_windows_total": overflow_windows_total,
                "papers_total": papers_total,
                "cooldown_active_total": cooldown_active_total,
                "skipped_windows_total": skipped_total,
                "rate_limited_windows_total": rate_limited_total,
                "failed_windows_total": failed_total,
                "budget_exhausted_windows_total": budget_exhausted_total,
                "cooldown_active": cooldown_active_total > 0,
                "cooldown_until": cooldown_until,
                "requests": entries,
            }
        )


class _Readiness:
    def __init__(
        self,
        *,
        raw_completed: bool,
        analysis_ready: bool,
        mature: bool,
        maturity_applicable: bool,
        maturity_cutoff: datetime | None,
        blocked_reason: str | None,
    ) -> None:
        self.raw_completed = raw_completed
        self.analysis_ready = analysis_ready
        self.mature = mature
        self.maturity_applicable = maturity_applicable
        self.maturity_cutoff = maturity_cutoff
        self.blocked_reason = blocked_reason


def _evaluate_readiness(request: ArxivRequest, settings: HuldraSettings) -> _Readiness:
    if request.submitted_start is None or request.submitted_end is None:
        return _Readiness(
            raw_completed=True,
            analysis_ready=True,
            mature=True,
            maturity_applicable=False,
            maturity_cutoff=None,
            blocked_reason=None,
        )
    maturity_lag_days = (
        settings.maturity_lag_days if request.maturity_lag_days is None else request.maturity_lag_days
    )
    if maturity_lag_days <= 0:
        return _Readiness(
            raw_completed=True,
            analysis_ready=True,
            mature=True,
            maturity_applicable=False,
            maturity_cutoff=None,
            blocked_reason=None,
        )
    now = ensure_utc(utc_now())
    today = datetime(now.year, now.month, now.day, tzinfo=UTC)
    cutoff = today - timedelta(days=max(0, maturity_lag_days - 1))
    mature = ensure_utc(request.submitted_end) <= cutoff
    return _Readiness(
        raw_completed=True,
        analysis_ready=mature,
        mature=mature,
        maturity_applicable=True,
        maturity_cutoff=cutoff,
        blocked_reason=None if mature else "immature_window",
    )


def _queue_item_has_terminal_outcome(item: QueueItem | None) -> bool:
    if item is None:
        return False
    if item.status not in _PENDING_QUEUE_STATUSES:
        return True
    return item.status == "delayed" and item.error_category in _TERMINAL_DELAYED_ERROR_CATEGORIES


def _raw_status_from_queue_item(item: QueueItem | None) -> str | None:
    if item is None:
        return None
    if item.status == "delayed" and item.error_category == "cooldown":
        return "skipped"
    if item.status == "delayed" and item.error_category == "rate_limited":
        return "rate_limited"
    return str(item.status)


def _hash_token(value: str | None) -> str | None:
    if value is None:
        return None
    return sha256(value.encode()).hexdigest()


def _oai_scope_lease_name(request: OaiHarvestRequest) -> str:
    if _should_commit_oai_watermark(request):
        scope_parts = (
            request.metadata_prefix,
            request.set_spec or "",
            "shared-watermark-writer",
        )
    else:
        scope_parts = (
            request.metadata_prefix,
            request.set_spec or "",
            request.from_datestamp or "",
            request.until_datestamp or "",
            str(request.mode),
        )
    scope = "\0".join(scope_parts)
    return f"oai_harvest:{sha256(scope.encode()).hexdigest()}"


def _oai_budget_error(
    request: OaiHarvestRequest,
    *,
    pages_total: int,
    requests_total: int,
    deadline_at: datetime | None,
) -> str | None:
    if pages_total >= request.max_pages:
        return "page_budget_exceeded"
    if requests_total >= request.max_requests:
        return "request_budget_exceeded"
    if deadline_at is not None and utc_now() >= deadline_at:
        return "runtime_budget_exceeded"
    return None


def _oai_token_error(
    *,
    records_total: int,
    consumed_token_hash: str | None,
    next_token_hash: str | None,
    consumed_token_hashes: set[str],
) -> str | None:
    if next_token_hash is None:
        return None
    if consumed_token_hash is not None and next_token_hash == consumed_token_hash:
        return "repeated_resumption_token"
    if next_token_hash in consumed_token_hashes:
        return "resumption_token_cycle"
    if records_total == 0:
        return "no_progress"
    return None


def _max_datestamp_seen(records: Sequence[OaiRecord], current: str | None) -> str | None:
    best = current
    for record in records:
        datestamp = getattr(record, "datestamp", None)
        if datestamp is None:
            continue
        value = ensure_utc(datestamp).date().isoformat()
        if best is None or value > best:
            best = value
    return best


def _should_commit_oai_watermark(request: OaiHarvestRequest) -> bool:
    return request.from_datestamp is None and request.until_datestamp is None


def _normalize_oai_day_datestamp(
    value: str | None,
    *,
    keep_invalid: bool = True,
) -> str | None:
    if value is None:
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return raw if keep_invalid else None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).date().isoformat()


def _subtract_oai_overlap(value: str, seconds: int) -> str:
    raw = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return (parsed.astimezone(UTC) - timedelta(seconds=seconds)).date().isoformat()


def _unreadable_completed_raw_status(item: QueueItem | None) -> str:
    status = _raw_status_from_queue_item(item)
    if status is None or status == "completed":
        return "cache_unreadable"
    return status


def _is_pure_id_list_request(request: ArxivRequest) -> bool:
    return (
        bool(request.id_list)
        and request.search_query is None
        and request.submitted_start is None
        and request.submitted_end is None
        and request.start == 0
        and request.sort_by == "submittedDate"
        and request.sort_order == "descending"
        and request.max_results >= len(request.id_list)
    )


def _maintenance_timeout_seconds(
    requests: list[ArxivRequest],
    settings: HuldraSettings,
    wait_timeout_seconds: float | None,
) -> float:
    if wait_timeout_seconds is not None:
        return wait_timeout_seconds
    request_timeouts = [
        request.timeout_seconds for request in requests if request.timeout_seconds is not None
    ]
    if request_timeouts:
        return max(request_timeouts)
    return settings.request_timeout_seconds


def _deadline_can_fit_requests(
    deadline: float,
    requests_total: int,
    interval_seconds: float,
    *,
    first_request_may_start_now: bool,
) -> bool:
    intervals_total = max(0, requests_total - 1) if first_request_may_start_now else requests_total
    minimum_seconds = intervals_total * interval_seconds
    return time.monotonic() + minimum_seconds <= deadline


def _count_inline_worker_result(
    result: HuldraMaintenanceResult,
    worker_result: WorkerPassResult,
) -> HuldraMaintenanceResult:
    updates: dict[str, object] = {}
    if worker_result.status in _INLINE_UPSTREAM_STATUSES:
        updates["upstream_requests_total"] = result.upstream_requests_total + 1
    if worker_result.status == "rate_limited":
        updates["upstream_429_total"] = result.upstream_429_total + 1
        updates["retry_after_seconds"] = worker_result.retry_after_seconds
    return result.model_copy(update=updates) if updates else result
