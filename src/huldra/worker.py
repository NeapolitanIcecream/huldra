from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol
from uuid import uuid4

from loguru import logger

from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.fetcher import (
    ArxivApiFetcher,
    FetchResult,
    HuldraFetchError,
    NonRetryableFetchError,
    RateLimitedError,
    TransientFetchError,
)
from huldra.keys import normalize_arxiv_id
from huldra.limiter import HuldraRateLimiter
from huldra.models import ArxivPaper, ArxivRequest, CachePolicy, QueueItem, QueueWorkKind, RequestStatus
from huldra.time import utc_now

log = logger.bind(module="huldra.worker")


class Fetcher(Protocol):
    def fetch(self, request: ArxivRequest) -> FetchResult: ...


@dataclass(frozen=True, slots=True)
class WorkerPassResult:
    status: str
    request_id: str | None = None
    cache_key: str | None = None
    papers_total: int = 0
    cooldown_until: datetime | None = None
    retry_after_seconds: int | None = None
    error_category: str | None = None
    error_message: str | None = None

    def as_payload(self) -> dict[str, object]:
        return {
            "status": self.status,
            "request_id": self.request_id,
            "cache_key": self.cache_key,
            "papers_total": self.papers_total,
            "cooldown_until": self.cooldown_until.isoformat() if self.cooldown_until else None,
            "retry_after_seconds": self.retry_after_seconds,
            "error_category": self.error_category,
            "error_message": self.error_message,
        }


@dataclass(frozen=True, slots=True)
class _IdListFetchPlan:
    requested_ids: tuple[str, ...]
    fetch_request: ArxivRequest | None = None
    reserved_ids: tuple[str, ...] = ()
    papers_by_id: dict[str, ArxivPaper] | None = None
    blocked_until: datetime | None = None


class HuldraWorker:
    def __init__(
        self,
        store: HuldraStore,
        settings: HuldraSettings,
        *,
        fetcher: Fetcher | None = None,
        limiter: HuldraRateLimiter | None = None,
        owner_token: str | None = None,
        sleep: Callable[[float], object] | None = None,
        name: str = "default",
    ) -> None:
        self.store = store
        self.settings = settings
        self.fetcher = fetcher or ArxivApiFetcher(settings)
        self.limiter = limiter or HuldraRateLimiter(store, settings)
        self.owner_token = owner_token or str(uuid4())
        self.sleep = sleep or time.sleep
        self.name = name

    def run_once(
        self,
        *,
        target_cache_keys: frozenset[str] | set[str] | None = None,
    ) -> WorkerPassResult:
        self.store.init_schema()
        self.store.record_worker_started(name=self.name)
        item = self.store.claim_next_queue_item(
            owner_token=self.owner_token,
            claim_timeout_seconds=self.settings.queue_claim_timeout_seconds,
            cache_keys=target_cache_keys,
        )
        if item is None:
            next_wake = utc_now() + timedelta(seconds=self.settings.worker_poll_interval_seconds)
            self.store.record_worker_completed(name=self.name, next_wake_at=next_wake)
            return WorkerPassResult(status="idle")

        cached = self.store.get_readable_completed_cache(item.cache_key)
        if cached is not None and item.work_kind == QueueWorkKind.FETCH_MISSING:
            self.store.complete_queue_item(item.request_id)
            self.store.record_worker_completed(name=self.name)
            return WorkerPassResult(
                status="cache_hit",
                request_id=item.request_id,
                cache_key=item.cache_key,
                papers_total=cached.result_count,
            )

        id_plan = self._plan_id_list_fetch(item)
        if id_plan is not None:
            if id_plan.fetch_request is None and id_plan.blocked_until is None:
                assert id_plan.papers_by_id is not None
                papers = [id_plan.papers_by_id[arxiv_id] for arxiv_id in id_plan.requested_ids]
                self.store.record_completed_cache_entry(
                    cache_key=item.cache_key,
                    request=item.request,
                    papers=papers,
                    total_results=len(papers),
                    upstream_request_count=0,
                )
                self.store.complete_queue_item(item.request_id)
                self.store.record_worker_completed(name=self.name)
                return WorkerPassResult(
                    status="cache_hit",
                    request_id=item.request_id,
                    cache_key=item.cache_key,
                    papers_total=len(papers),
                )
            if id_plan.blocked_until is not None:
                self.store.release_or_delay_queue_item(
                    item.request_id,
                    next_attempt_at=id_plan.blocked_until,
                    error_category="id_fetch_reserved",
                    error_message="id fetch reserved by another worker",
                )
                self.store.record_worker_completed(
                    name=self.name,
                    next_wake_at=id_plan.blocked_until,
                    error_category="id_fetch_reserved",
                    error_message="id fetch reserved by another worker",
                )
                return WorkerPassResult(
                    status="blocked",
                    request_id=item.request_id,
                    cache_key=item.cache_key,
                    cooldown_until=id_plan.blocked_until,
                    error_category="id_fetch_reserved",
                )

        decision = self.limiter.before_request(owner_token=self.owner_token)
        if not decision.can_fetch:
            next_attempt = decision.cooldown_until or (
                utc_now() + timedelta(seconds=self.settings.worker_poll_interval_seconds)
            )
            self.store.release_or_delay_queue_item(
                item.request_id,
                next_attempt_at=next_attempt,
                error_category=decision.blocked_reason,
                error_message=decision.blocked_reason,
            )
            self._release_id_plan(id_plan)
            self.store.record_worker_completed(
                name=self.name,
                next_wake_at=next_attempt,
                error_category=decision.blocked_reason,
                error_message=decision.blocked_reason,
            )
            return WorkerPassResult(
                status="cooling_down" if decision.blocked_reason == "cooldown" else "blocked",
                request_id=item.request_id,
                cache_key=item.cache_key,
                cooldown_until=decision.cooldown_until,
                error_category=decision.blocked_reason,
            )

        if decision.wait_seconds > 0:
            sleeper = self.sleep
            assert callable(sleeper)
            sleeper(decision.wait_seconds)

        fetch_request = (
            id_plan.fetch_request
            if id_plan is not None and id_plan.fetch_request is not None
            else item.request
        )
        try:
            result = self.fetcher.fetch(fetch_request)
        except RateLimitedError as exc:
            cooldown_until = self.limiter.after_429(
                owner_token=self.owner_token,
                retry_after_seconds=exc.retry_after_seconds,
            )
            self.store.record_rate_limited(
                cache_key=item.cache_key,
                request=item.request,
                cooldown_until=cooldown_until,
                error_message=str(exc),
            )
            self.store.release_or_delay_queue_item(
                item.request_id,
                next_attempt_at=cooldown_until,
                error_category="rate_limited",
                error_message=str(exc),
            )
            self._release_id_plan(id_plan)
            self.store.record_worker_completed(
                name=self.name,
                next_wake_at=cooldown_until,
                error_category="rate_limited",
                error_message=str(exc),
            )
            log.bind(cache_key=item.cache_key, cooldown_until=cooldown_until.isoformat()).warning(
                "fetch_rate_limited"
            )
            return WorkerPassResult(
                status="rate_limited",
                request_id=item.request_id,
                cache_key=item.cache_key,
                cooldown_until=cooldown_until,
                retry_after_seconds=exc.retry_after_seconds,
                error_category="rate_limited",
                error_message=str(exc),
            )
        except TransientFetchError as exc:
            self.limiter.after_failure(
                owner_token=self.owner_token,
                status=exc.status_code,
                error_message=str(exc),
            )
            next_attempt = utc_now() + timedelta(seconds=_backoff_seconds(item.attempts_total))
            self.store.record_cache_failure(
                cache_key=item.cache_key,
                request=item.request,
                error_category="transient",
                error_message=str(exc),
                upstream_status=exc.status_code,
            )
            self.store.release_or_delay_queue_item(
                item.request_id,
                next_attempt_at=next_attempt,
                error_category="transient",
                error_message=str(exc),
            )
            self._release_id_plan(id_plan)
            self.store.record_worker_completed(
                name=self.name,
                next_wake_at=next_attempt,
                error_category="transient",
                error_message=str(exc),
            )
            log.bind(cache_key=item.cache_key, status_code=exc.status_code).warning("fetch_transient_failure")
            return WorkerPassResult(
                status="transient_failure",
                request_id=item.request_id,
                cache_key=item.cache_key,
                cooldown_until=next_attempt,
                error_category="transient",
                error_message=str(exc),
            )
        except NonRetryableFetchError as exc:
            self.limiter.after_failure(
                owner_token=self.owner_token,
                status=exc.status_code,
                error_message=str(exc),
            )
            self.store.record_cache_failure(
                cache_key=item.cache_key,
                request=item.request,
                error_category="non_retryable",
                error_message=str(exc),
                upstream_status=exc.status_code,
            )
            self.store.release_or_delay_queue_item(
                item.request_id,
                status=RequestStatus.FAILED,
                error_category="non_retryable",
                error_message=str(exc),
            )
            self._release_id_plan(id_plan)
            self.store.record_worker_completed(
                name=self.name,
                error_category="non_retryable",
                error_message=str(exc),
            )
            return WorkerPassResult(
                status="failed",
                request_id=item.request_id,
                cache_key=item.cache_key,
                error_category="non_retryable",
                error_message=str(exc),
            )
        except HuldraFetchError as exc:
            self.limiter.after_failure(
                owner_token=self.owner_token,
                status=exc.status_code,
                error_message=str(exc),
            )
            self._release_id_plan(id_plan)
            self.store.record_worker_completed(
                name=self.name,
                error_category="fetch_error",
                error_message=str(exc),
            )
            raise

        papers = result.papers
        total_results = result.total_results
        if id_plan is not None:
            papers_by_id = dict(id_plan.papers_by_id or {})
            papers_by_id.update({paper.arxiv_id: paper for paper in result.papers})
            missing_after_fetch = [
                arxiv_id for arxiv_id in id_plan.requested_ids if arxiv_id not in papers_by_id
            ]
            if missing_after_fetch:
                self.limiter.after_failure(
                    owner_token=self.owner_token,
                    status=result.upstream_status,
                    error_message="upstream response omitted requested IDs",
                )
                self.store.record_cache_failure(
                    cache_key=item.cache_key,
                    request=item.request,
                    error_category="non_retryable",
                    error_message="upstream response omitted requested IDs",
                    upstream_status=result.upstream_status,
                )
                self.store.release_or_delay_queue_item(
                    item.request_id,
                    status=RequestStatus.FAILED,
                    error_category="non_retryable",
                    error_message="upstream response omitted requested IDs",
                )
                self._release_id_plan(id_plan)
                self.store.record_worker_completed(
                    name=self.name,
                    error_category="non_retryable",
                    error_message="upstream response omitted requested IDs",
                )
                return WorkerPassResult(
                    status="failed",
                    request_id=item.request_id,
                    cache_key=item.cache_key,
                    error_category="non_retryable",
                    error_message="upstream response omitted requested IDs",
                )
            papers = [papers_by_id[arxiv_id] for arxiv_id in id_plan.requested_ids]
            total_results = len(papers)

        self.store.record_completed_cache_entry(
            cache_key=item.cache_key,
            request=item.request,
            papers=papers,
            total_results=total_results,
            upstream_status=result.upstream_status,
        )
        self.limiter.after_success(owner_token=self.owner_token, status=result.upstream_status)
        self.store.complete_queue_item(item.request_id)
        self._release_id_plan(id_plan)
        self.store.record_worker_completed(name=self.name)
        return WorkerPassResult(
            status="completed",
            request_id=item.request_id,
            cache_key=item.cache_key,
            papers_total=len(papers),
        )

    def _plan_id_list_fetch(self, item: QueueItem) -> _IdListFetchPlan | None:
        if item.work_kind != QueueWorkKind.FETCH_MISSING or not _is_pure_id_list_request(item.request):
            return None
        requested_ids = tuple(normalize_arxiv_id(value) for value in item.request.id_list)
        papers_by_id = self.store.get_papers_by_ids(requested_ids)
        missing = tuple(arxiv_id for arxiv_id in requested_ids if arxiv_id not in papers_by_id)
        if not missing:
            return _IdListFetchPlan(requested_ids=requested_ids, papers_by_id=papers_by_id)
        reservations = self.store.acquire_id_fetch_reservations(
            missing,
            owner_token=self.owner_token,
            request_id=item.request_id,
            ttl_seconds=self.settings.queue_claim_timeout_seconds,
        )
        if reservations.blocked_until is not None:
            return _IdListFetchPlan(
                requested_ids=requested_ids,
                papers_by_id=papers_by_id,
                blocked_until=reservations.blocked_until,
            )
        return _IdListFetchPlan(
            requested_ids=requested_ids,
            fetch_request=item.request.model_copy(
                update={
                    "id_list": reservations.acquired_ids,
                    "cache_policy": CachePolicy.CACHE_OR_ENQUEUE,
                }
            ),
            reserved_ids=reservations.acquired_ids,
            papers_by_id=papers_by_id,
        )

    def _release_id_plan(self, plan: _IdListFetchPlan | None) -> None:
        if plan is not None and plan.reserved_ids:
            self.store.release_id_fetch_reservations(
                plan.reserved_ids,
                owner_token=self.owner_token,
            )


def _backoff_seconds(attempts_total: int) -> int:
    return min(3600, max(5, 2 ** max(1, attempts_total)))


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
