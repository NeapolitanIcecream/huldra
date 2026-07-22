from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from math import ceil
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
from huldra.keys import arxiv_version, normalize_arxiv_id
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

        ownership_ttl = self._fetch_ownership_ttl(item)
        id_plan, ownership_result = self._gate_fetch_ownership(
            item,
            id_plan,
            ttl_seconds=ownership_ttl,
        )
        if ownership_result is not None:
            return ownership_result

        budget_error = (
            self.store.reserve_upstream_request(item.upstream_budget_id)
            if item.upstream_budget_id is not None
            else None
        )
        if budget_error is None and item.upstream_budget_id is not None:
            budget_error = self.store.check_upstream_request_deadline(
                item.upstream_budget_id
            )
        if budget_error is not None:
            self.store.release_lease(self.limiter.lease_name, self.owner_token)
            error_message = budget_error.replace("_", " ")
            self.store.record_cache_failure(
                cache_key=item.cache_key,
                request=item.request,
                error_category=budget_error,
                error_message=error_message,
                upstream_request_count=0,
            )
            self.store.release_or_delay_queue_item(
                item.request_id,
                status=RequestStatus.FAILED,
                error_category=budget_error,
                error_message=error_message,
            )
            self._release_id_plan(id_plan)
            self.store.record_worker_completed(
                name=self.name,
                error_category=budget_error,
                error_message=error_message,
            )
            return WorkerPassResult(
                status="budget_exceeded",
                request_id=item.request_id,
                cache_key=item.cache_key,
                error_category=budget_error,
                error_message=error_message,
            )
        # Durable budget accounting may itself wait on SQLite long enough for
        # every earlier TTL to expire. Conservatively consumed budget remains
        # consumed if this final ownership fence decides not to issue I/O.
        id_plan, ownership_result = self._gate_fetch_ownership(
            item,
            id_plan,
            ttl_seconds=ownership_ttl,
        )
        if ownership_result is not None:
            return ownership_result
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
                status_code=exc.status_code or 429,
                error_message=str(exc),
            )
            self.store.record_rate_limited(
                cache_key=item.cache_key,
                request=item.request,
                cooldown_until=cooldown_until,
                upstream_status=exc.status_code or 429,
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
            papers_by_id = _id_list_paper_lookup(id_plan.papers_by_id or {}, result.papers)
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
            papers = _ordered_id_list_papers(id_plan.requested_ids, papers_by_id)
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

    def _fetch_ownership_ttl(self, item: QueueItem) -> int:
        request_timeout = item.request.timeout_seconds or self.settings.request_timeout_seconds
        return max(
            self.settings.queue_claim_timeout_seconds,
            self.settings.lease_timeout_seconds,
            ceil(request_timeout + self.store.timeout + 5.0),
        )

    def _gate_fetch_ownership(
        self,
        item: QueueItem,
        id_plan: _IdListFetchPlan | None,
        *,
        ttl_seconds: int,
    ) -> tuple[_IdListFetchPlan | None, WorkerPassResult | None]:
        reason = self.store.renew_worker_fetch_ownership(
            request_id=item.request_id,
            owner_token=self.owner_token,
            lease_name=self.limiter.lease_name,
            ttl_seconds=ttl_seconds,
            reserved_ids=id_plan.reserved_ids if id_plan is not None else (),
        )
        if reason is not None:
            return id_plan, self._fetch_ownership_failure(item, id_plan, reason)

        cached = self.store.get_readable_completed_cache(item.cache_key)
        if cached is not None and item.work_kind == QueueWorkKind.FETCH_MISSING:
            self.store.release_lease(self.limiter.lease_name, self.owner_token)
            self.store.complete_queue_item(item.request_id)
            self._release_id_plan(id_plan)
            self.store.record_worker_completed(name=self.name)
            return id_plan, WorkerPassResult(
                status="cache_hit",
                request_id=item.request_id,
                cache_key=item.cache_key,
                papers_total=cached.result_count,
            )

        if id_plan is not None:
            id_plan = self._revalidate_id_list_fetch_plan(
                item,
                id_plan,
                ttl_seconds=ttl_seconds,
            )
            if id_plan.blocked_until is not None:
                self.store.release_lease(self.limiter.lease_name, self.owner_token)
                self.store.release_or_delay_queue_item(
                    item.request_id,
                    next_attempt_at=id_plan.blocked_until,
                    error_category="id_fetch_reserved",
                    error_message="id fetch reservation changed before network I/O",
                )
                self._release_id_plan(id_plan)
                self.store.record_worker_completed(
                    name=self.name,
                    next_wake_at=id_plan.blocked_until,
                    error_category="id_fetch_reserved",
                    error_message="id fetch reservation changed before network I/O",
                )
                return id_plan, WorkerPassResult(
                    status="blocked",
                    request_id=item.request_id,
                    cache_key=item.cache_key,
                    cooldown_until=id_plan.blocked_until,
                    error_category="id_fetch_reserved",
                )
            if id_plan.fetch_request is None:
                assert id_plan.papers_by_id is not None
                papers = [
                    id_plan.papers_by_id[arxiv_id]
                    for arxiv_id in id_plan.requested_ids
                ]
                self.store.release_lease(self.limiter.lease_name, self.owner_token)
                self.store.record_completed_cache_entry(
                    cache_key=item.cache_key,
                    request=item.request,
                    papers=papers,
                    total_results=len(papers),
                    upstream_request_count=0,
                )
                self.store.complete_queue_item(item.request_id)
                self._release_id_plan(id_plan)
                self.store.record_worker_completed(name=self.name)
                return id_plan, WorkerPassResult(
                    status="cache_hit",
                    request_id=item.request_id,
                    cache_key=item.cache_key,
                    papers_total=len(papers),
                )

        # The cache and ID checks use their own transactions. End with one
        # atomic fence so no SQLite wait can leave mixed ownership at fetch time.
        reason = self.store.renew_worker_fetch_ownership(
            request_id=item.request_id,
            owner_token=self.owner_token,
            lease_name=self.limiter.lease_name,
            ttl_seconds=ttl_seconds,
            reserved_ids=id_plan.reserved_ids if id_plan is not None else (),
        )
        if reason is not None:
            return id_plan, self._fetch_ownership_failure(item, id_plan, reason)
        return id_plan, None

    def _fetch_ownership_failure(
        self,
        item: QueueItem,
        id_plan: _IdListFetchPlan | None,
        reason: str,
    ) -> WorkerPassResult:
        self.store.release_lease(self.limiter.lease_name, self.owner_token)
        if reason == "id_fetch_reserved":
            next_attempt = utc_now() + timedelta(
                seconds=self.settings.worker_poll_interval_seconds
            )
            self.store.release_or_delay_queue_item(
                item.request_id,
                next_attempt_at=next_attempt,
                error_category=reason,
                error_message="id fetch reservation changed before network I/O",
            )
            self._release_id_plan(id_plan)
            self.store.record_worker_completed(
                name=self.name,
                next_wake_at=next_attempt,
                error_category=reason,
                error_message="id fetch reservation changed before network I/O",
            )
            return WorkerPassResult(
                status="blocked",
                request_id=item.request_id,
                cache_key=item.cache_key,
                cooldown_until=next_attempt,
                error_category=reason,
            )

        self._release_id_plan(id_plan)
        error_message = (
            "upstream lease changed before network I/O"
            if reason == "lost_lease"
            else "queue claim changed before network I/O"
        )
        self.store.record_worker_completed(
            name=self.name,
            error_category=reason,
            error_message=error_message,
        )
        return WorkerPassResult(
            status=reason,
            request_id=item.request_id,
            cache_key=item.cache_key,
            error_category=reason,
            error_message=error_message,
        )

    def _plan_id_list_fetch(self, item: QueueItem) -> _IdListFetchPlan | None:
        if item.work_kind != QueueWorkKind.FETCH_MISSING or not _is_pure_id_list_request(item.request):
            return None
        requested_ids = tuple(dict.fromkeys(normalize_arxiv_id(value) for value in item.request.id_list))
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

    def _revalidate_id_list_fetch_plan(
        self,
        item: QueueItem,
        plan: _IdListFetchPlan,
        *,
        ttl_seconds: int,
    ) -> _IdListFetchPlan:
        if not plan.reserved_ids:
            return plan
        papers_by_id = self.store.get_papers_by_ids(plan.requested_ids)
        missing = tuple(
            arxiv_id for arxiv_id in plan.requested_ids if arxiv_id not in papers_by_id
        )
        no_longer_missing = tuple(
            arxiv_id for arxiv_id in plan.reserved_ids if arxiv_id not in missing
        )
        if no_longer_missing:
            self.store.release_id_fetch_reservations(
                no_longer_missing,
                owner_token=self.owner_token,
            )
        still_reserved = tuple(
            arxiv_id for arxiv_id in plan.reserved_ids if arxiv_id in missing
        )
        if not missing:
            return _IdListFetchPlan(
                requested_ids=plan.requested_ids,
                papers_by_id=papers_by_id,
            )
        if set(still_reserved) != set(missing) or not self.store.renew_id_fetch_reservations(
            still_reserved,
            owner_token=self.owner_token,
            ttl_seconds=ttl_seconds,
        ):
            return _IdListFetchPlan(
                requested_ids=plan.requested_ids,
                reserved_ids=still_reserved,
                papers_by_id=papers_by_id,
                blocked_until=utc_now()
                + timedelta(seconds=self.settings.worker_poll_interval_seconds),
            )
        return _IdListFetchPlan(
            requested_ids=plan.requested_ids,
            fetch_request=item.request.model_copy(
                update={
                    "id_list": still_reserved,
                    "cache_policy": CachePolicy.CACHE_OR_ENQUEUE,
                }
            ),
            reserved_ids=still_reserved,
            papers_by_id=papers_by_id,
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


def _id_list_paper_lookup(
    cached_papers_by_id: dict[str, ArxivPaper],
    fetched_papers: list[ArxivPaper],
) -> dict[str, ArxivPaper]:
    lookup = dict(cached_papers_by_id)
    for paper in cached_papers_by_id.values():
        _add_id_list_paper_lookup(lookup, paper)
    for paper in fetched_papers:
        _add_id_list_paper_lookup(lookup, paper)
    return lookup


def _add_id_list_paper_lookup(lookup: dict[str, ArxivPaper], paper: ArxivPaper) -> None:
    arxiv_id = normalize_arxiv_id(paper.arxiv_id)
    lookup[arxiv_id] = paper
    versionless_id = _versionless_arxiv_id(arxiv_id)
    if versionless_id != arxiv_id:
        lookup.setdefault(versionless_id, paper)


def _ordered_id_list_papers(
    requested_ids: tuple[str, ...],
    papers_by_id: dict[str, ArxivPaper],
) -> list[ArxivPaper]:
    papers: list[ArxivPaper] = []
    seen: set[str] = set()
    for arxiv_id in requested_ids:
        paper = papers_by_id[arxiv_id]
        if paper.arxiv_id in seen:
            continue
        seen.add(paper.arxiv_id)
        papers.append(paper)
    return papers


def _versionless_arxiv_id(arxiv_id: str) -> str:
    version = arxiv_version(arxiv_id)
    if version is None:
        return arxiv_id
    return arxiv_id[: -len(f"v{version}")]
