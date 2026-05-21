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
from huldra.limiter import HuldraRateLimiter
from huldra.models import ArxivRequest, RequestStatus
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
    error_category: str | None = None
    error_message: str | None = None

    def as_payload(self) -> dict[str, object]:
        return {
            "status": self.status,
            "request_id": self.request_id,
            "cache_key": self.cache_key,
            "papers_total": self.papers_total,
            "cooldown_until": self.cooldown_until.isoformat() if self.cooldown_until else None,
            "error_category": self.error_category,
            "error_message": self.error_message,
        }


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

    def run_once(self) -> WorkerPassResult:
        self.store.init_schema()
        self.store.record_worker_started(name=self.name)
        item = self.store.claim_next_queue_item(
            owner_token=self.owner_token,
            claim_timeout_seconds=self.settings.queue_claim_timeout_seconds,
        )
        if item is None:
            next_wake = utc_now() + timedelta(seconds=self.settings.worker_poll_interval_seconds)
            self.store.record_worker_completed(name=self.name, next_wake_at=next_wake)
            return WorkerPassResult(status="idle")

        cached = self.store.get_cache_entry(item.cache_key)
        if cached is not None and cached.status == "completed":
            self.store.complete_queue_item(item.request_id)
            return WorkerPassResult(
                status="cache_hit",
                request_id=item.request_id,
                cache_key=item.cache_key,
                papers_total=cached.result_count,
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

        try:
            result = self.fetcher.fetch(item.request)
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
            log.bind(cache_key=item.cache_key, cooldown_until=cooldown_until.isoformat()).warning(
                "fetch_rate_limited"
            )
            return WorkerPassResult(
                status="rate_limited",
                request_id=item.request_id,
                cache_key=item.cache_key,
                cooldown_until=cooldown_until,
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
            log.bind(cache_key=item.cache_key, status_code=exc.status_code).warning("fetch_transient_failure")
            return WorkerPassResult(
                status="transient_failure",
                request_id=item.request_id,
                cache_key=item.cache_key,
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
            raise

        self.store.record_completed_cache_entry(
            cache_key=item.cache_key,
            request=item.request,
            papers=result.papers,
            total_results=result.total_results,
            upstream_status=result.upstream_status,
        )
        self.limiter.after_success(owner_token=self.owner_token, status=result.upstream_status)
        self.store.complete_queue_item(item.request_id)
        self.store.record_worker_completed(name=self.name)
        return WorkerPassResult(
            status="completed",
            request_id=item.request_id,
            cache_key=item.cache_key,
            papers_total=len(result.papers),
        )


def _backoff_seconds(attempts_total: int) -> int:
    return min(3600, max(5, 2 ** max(1, attempts_total)))
