from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from math import ceil
from random import uniform

from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.models import RateState
from huldra.time import ensure_utc, isoformat_or_none, utc_now


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    can_fetch: bool
    wait_seconds: float = 0.0
    blocked_reason: str | None = None
    cooldown_until: datetime | None = None
    lease_acquired: bool = False
    request_started_at: datetime | None = None


class HuldraRateLimiter:
    def __init__(
        self,
        store: HuldraStore,
        settings: HuldraSettings,
        *,
        name: str = "arxiv_legacy_api",
        lease_name: str = "upstream_fetch",
        jitter: Callable[[float], float] | None = None,
    ) -> None:
        self.store = store
        self.settings = settings
        self.name = name
        self.lease_name = lease_name
        self._jitter = jitter or (lambda maximum: uniform(0.0, maximum))

    def seconds_until_next_request(self, *, now: datetime | None = None) -> float:
        current = ensure_utc(now or utc_now())
        state = self.store.get_rate_state(self.name)
        return self._seconds_until_next_request(state, now=current)

    def cooldown_active(self, *, now: datetime | None = None) -> bool:
        current = ensure_utc(now or utc_now())
        state = self.store.get_rate_state(self.name)
        return state.cooldown_until is not None and state.cooldown_until > current

    def before_request(
        self,
        *,
        owner_token: str,
        now: datetime | None = None,
    ) -> RateLimitDecision:
        explicit_now = ensure_utc(now) if now is not None else None
        acquired = self.store.acquire_lease(
            self.lease_name,
            owner_token,
            self.settings.lease_timeout_seconds,
            now=explicit_now,
        )
        if not acquired:
            return RateLimitDecision(can_fetch=False, blocked_reason="lease_held")

        # Production callers deliberately let the store sample time after its
        # BEGIN IMMEDIATE lock succeeds. Sampling before SQLite lock wait can
        # create a lease that is already expired when this method returns.
        current = explicit_now or ensure_utc(utc_now())
        # Read the durable limit only after winning the shared lease. This closes
        # the race where another worker persists a cooldown between an unlocked
        # state read and lease acquisition.
        state = self.store.get_rate_state(self.name)
        if state.cooldown_until is not None and state.cooldown_until > current:
            self.store.release_lease(self.lease_name, owner_token)
            return RateLimitDecision(
                can_fetch=False,
                blocked_reason="cooldown",
                cooldown_until=state.cooldown_until,
            )

        wait_seconds = self._seconds_until_next_request(state, now=current)
        lease_timeout_seconds = max(
            self.settings.lease_timeout_seconds,
            ceil(
                wait_seconds
                + self.settings.request_timeout_seconds
                + self.store.timeout
                + 5.0
            ),
        )
        # Strict renewal cannot silently reacquire a lease that another process
        # took and released while the durable rate state was being read.
        renewed = self.store.renew_lease_if_owned(
            self.lease_name,
            owner_token,
            lease_timeout_seconds,
            now=explicit_now,
        )
        if not renewed:  # pragma: no cover - only an external lease mutation can cause this
            return RateLimitDecision(can_fetch=False, blocked_reason="lease_held")
        request_started_at = current + timedelta(seconds=wait_seconds)
        api_family = _api_family(owner_token)
        updated = state.model_copy(
            update={
                "last_request_started_at": request_started_at,
                "last_rate_wait_seconds": wait_seconds,
                "last_request_latency_ms": None,
                "last_api_family": api_family,
            }
        )
        try:
            self.store.set_rate_state(updated)
            self.store.record_event(
                "upstream_request_started",
                {
                    "api_family": api_family,
                    "request_started_at": isoformat_or_none(request_started_at),
                    "rate_wait_seconds": wait_seconds,
                },
            )
        except Exception:
            self.store.release_lease(self.lease_name, owner_token)
            raise
        return RateLimitDecision(
            can_fetch=True,
            wait_seconds=wait_seconds,
            lease_acquired=True,
            request_started_at=request_started_at,
        )

    def after_success(
        self,
        *,
        owner_token: str,
        status: int = 200,
        now: datetime | None = None,
    ) -> None:
        current = ensure_utc(now or utc_now())
        previous = self.store.get_rate_state(self.name)
        diagnostics = _request_diagnostics(previous, current=current, owner_token=owner_token)
        self.store.set_rate_state(
            previous.model_copy(
                update={
                    "last_request_at": current,
                    "cooldown_until": None,
                    "consecutive_429_total": 0,
                    "consecutive_rate_limit_total": 0,
                    "last_status": status,
                    "last_error_message": None,
                    "last_request_latency_ms": diagnostics["latency_ms"],
                    "last_retry_after_seconds": None,
                    "last_effective_cooldown_seconds": None,
                    "last_rate_limit_kind": None,
                    "last_api_family": diagnostics["api_family"],
                }
            )
        )
        try:
            self.store.record_event(
                "upstream_request_finished",
                {
                    **diagnostics,
                    "outcome": "success",
                    "status": status,
                    "rate_limit_kind": None,
                    "retry_after_seconds": None,
                    "effective_cooldown_seconds": None,
                    "consecutive_rate_limit_total": 0,
                },
            )
        finally:
            self.store.release_lease(self.lease_name, owner_token)

    def after_rate_limit(
        self,
        *,
        owner_token: str,
        retry_after_seconds: int | None,
        status_code: int = 429,
        rate_limit_kind: str | None = None,
        api_family: str | None = None,
        error_message: str | None = None,
        now: datetime | None = None,
    ) -> datetime:
        current = ensure_utc(now or utc_now())
        previous = self.store.get_rate_state(self.name)
        consecutive = previous.consecutive_rate_limit_total + 1
        effective_seconds = self._effective_cooldown_seconds(
            retry_after_seconds=retry_after_seconds,
            consecutive_rate_limit_total=consecutive,
        )
        cooldown_until = current + timedelta(seconds=effective_seconds)
        resolved_kind = rate_limit_kind or _rate_limit_kind(status_code)
        resolved_family = api_family or _api_family(owner_token)
        diagnostics = _request_diagnostics(
            previous,
            current=current,
            owner_token=owner_token,
            api_family=resolved_family,
        )
        is_http_429 = status_code == 429
        is_oai_503_retry_after = resolved_kind == "oai_503_retry_after"
        self.store.set_rate_state(
            previous.model_copy(
                update={
                    "last_request_at": current,
                    "cooldown_until": cooldown_until,
                    "consecutive_429_total": (
                        previous.consecutive_429_total + 1 if is_http_429 else 0
                    ),
                    "upstream_429_total": (
                        previous.upstream_429_total + (1 if is_http_429 else 0)
                    ),
                    "consecutive_rate_limit_total": consecutive,
                    "upstream_rate_limited_total": previous.upstream_rate_limited_total + 1,
                    "upstream_oai_503_retry_after_total": (
                        previous.upstream_oai_503_retry_after_total
                        + (1 if is_oai_503_retry_after else 0)
                    ),
                    "last_status": status_code,
                    "last_error_message": error_message or f"arXiv returned HTTP {status_code}",
                    "last_request_latency_ms": diagnostics["latency_ms"],
                    "last_retry_after_seconds": retry_after_seconds,
                    "last_effective_cooldown_seconds": effective_seconds,
                    "last_rate_limit_kind": resolved_kind,
                    "last_api_family": resolved_family,
                }
            )
        )
        try:
            self.store.record_event(
                "upstream_request_finished",
                {
                    **diagnostics,
                    "outcome": "rate_limited",
                    "status": status_code,
                    "rate_limit_kind": resolved_kind,
                    "retry_after_seconds": retry_after_seconds,
                    "effective_cooldown_seconds": effective_seconds,
                    "consecutive_rate_limit_total": consecutive,
                    "cooldown_until": isoformat_or_none(cooldown_until),
                },
            )
        finally:
            self.store.release_lease(self.lease_name, owner_token)
        return cooldown_until

    def after_429(
        self,
        *,
        owner_token: str,
        retry_after_seconds: int | None,
        status_code: int = 429,
        error_message: str | None = None,
        now: datetime | None = None,
    ) -> datetime:
        """Compatibility wrapper for callers using the original method name."""
        return self.after_rate_limit(
            owner_token=owner_token,
            retry_after_seconds=retry_after_seconds,
            status_code=status_code,
            error_message=error_message,
            now=now,
        )

    def after_failure(
        self,
        *,
        owner_token: str,
        status: int | None = None,
        error_message: str | None = None,
        now: datetime | None = None,
    ) -> None:
        current = ensure_utc(now or utc_now())
        previous = self.store.get_rate_state(self.name)
        diagnostics = _request_diagnostics(previous, current=current, owner_token=owner_token)
        self.store.set_rate_state(
            previous.model_copy(
                update={
                    "last_request_at": current,
                    "last_status": status,
                    "last_error_message": error_message,
                    "last_request_latency_ms": diagnostics["latency_ms"],
                    "last_retry_after_seconds": None,
                    "last_effective_cooldown_seconds": None,
                    "last_rate_limit_kind": None,
                    "last_api_family": diagnostics["api_family"],
                }
            )
        )
        try:
            self.store.record_event(
                "upstream_request_finished",
                {
                    **diagnostics,
                    "outcome": "failure",
                    "status": status,
                    "rate_limit_kind": None,
                    "retry_after_seconds": None,
                    "effective_cooldown_seconds": None,
                    "consecutive_rate_limit_total": previous.consecutive_rate_limit_total,
                },
            )
        finally:
            self.store.release_lease(self.lease_name, owner_token)

    def _seconds_until_next_request(self, state: RateState, *, now: datetime) -> float:
        if state.last_request_at is None:
            return 0.0
        target = state.last_request_at + timedelta(
            seconds=self.settings.request_interval_seconds
        )
        return max(0.0, (target - now).total_seconds())

    def _effective_cooldown_seconds(
        self,
        *,
        retry_after_seconds: int | None,
        consecutive_rate_limit_total: int,
    ) -> float:
        safety_floor = max(
            float(self.settings.cooldown_seconds),
            self.settings.request_interval_seconds,
        )
        exponent = max(0, consecutive_rate_limit_total - 1)
        try:
            adaptive = safety_floor * (
                self.settings.rate_limit_backoff_multiplier**exponent
            )
        except OverflowError:
            adaptive = self.settings.rate_limit_max_cooldown_seconds
        adaptive = min(adaptive, self.settings.rate_limit_max_cooldown_seconds)
        retry_after_floor = float(max(0, retry_after_seconds or 0))
        lower_bound = max(adaptive, retry_after_floor)
        jitter_max = self.settings.rate_limit_jitter_seconds
        sampled_jitter = float(self._jitter(jitter_max)) if jitter_max > 0 else 0.0
        upward_jitter = min(jitter_max, max(0.0, sampled_jitter))
        return lower_bound + upward_jitter


def _api_family(owner_token: str) -> str:
    return "oai_pmh" if owner_token.startswith("oai:") else "legacy_api"


def _rate_limit_kind(status_code: int) -> str:
    return "oai_503_retry_after" if status_code == 503 else "http_429"


def _request_diagnostics(
    state: RateState,
    *,
    current: datetime,
    owner_token: str,
    api_family: str | None = None,
) -> dict[str, str | float]:
    request_started_at = state.last_request_started_at or current
    latency_ms = max(0.0, (current - request_started_at).total_seconds() * 1000.0)
    return {
        "api_family": api_family or state.last_api_family or _api_family(owner_token),
        "request_started_at": isoformat_or_none(request_started_at) or current.isoformat(),
        "rate_wait_seconds": state.last_rate_wait_seconds or 0.0,
        "latency_ms": latency_ms,
    }
