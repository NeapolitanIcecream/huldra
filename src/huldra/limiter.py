from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from math import ceil

from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.models import RateState
from huldra.time import ensure_utc, utc_now


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    can_fetch: bool
    wait_seconds: float = 0.0
    blocked_reason: str | None = None
    cooldown_until: datetime | None = None
    lease_acquired: bool = False


class HuldraRateLimiter:
    def __init__(
        self,
        store: HuldraStore,
        settings: HuldraSettings,
        *,
        name: str = "arxiv_legacy_api",
        lease_name: str = "upstream_fetch",
    ) -> None:
        self.store = store
        self.settings = settings
        self.name = name
        self.lease_name = lease_name

    def seconds_until_next_request(self, *, now: datetime | None = None) -> float:
        current = ensure_utc(now or utc_now())
        state = self.store.get_rate_state(self.name)
        if state.last_request_at is None:
            return 0.0
        target = state.last_request_at + timedelta(seconds=self.settings.request_interval_seconds)
        return max(0.0, (target - current).total_seconds())

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
        current = ensure_utc(now or utc_now())
        state = self.store.get_rate_state(self.name)
        if state.cooldown_until is not None and state.cooldown_until > current:
            return RateLimitDecision(
                can_fetch=False,
                blocked_reason="cooldown",
                cooldown_until=state.cooldown_until,
            )
        wait_seconds = self.seconds_until_next_request(now=current)
        lease_timeout_seconds = max(
            self.settings.lease_timeout_seconds,
            ceil(wait_seconds + self.settings.request_timeout_seconds + 5.0),
        )
        acquired = self.store.acquire_lease(
            self.lease_name,
            owner_token,
            lease_timeout_seconds,
            now=current,
        )
        if not acquired:
            return RateLimitDecision(can_fetch=False, blocked_reason="lease_held")
        return RateLimitDecision(
            can_fetch=True,
            wait_seconds=wait_seconds,
            lease_acquired=True,
        )

    def after_success(
        self,
        *,
        owner_token: str,
        status: int = 200,
        now: datetime | None = None,
    ) -> None:
        previous = self.store.get_rate_state(self.name)
        self.store.set_rate_state(
            RateState(
                name=self.name,
                last_request_at=ensure_utc(now or utc_now()),
                cooldown_until=None,
                consecutive_429_total=0,
                upstream_429_total=previous.upstream_429_total,
                last_status=status,
                last_error_message=None,
            )
        )
        self.store.release_lease(self.lease_name, owner_token)

    def after_429(
        self,
        *,
        owner_token: str,
        retry_after_seconds: int | None,
        status_code: int = 429,
        error_message: str | None = None,
        now: datetime | None = None,
    ) -> datetime:
        current = ensure_utc(now or utc_now())
        seconds = self.settings.cooldown_seconds if retry_after_seconds is None else retry_after_seconds
        previous = self.store.get_rate_state(self.name)
        cooldown_until = current + timedelta(seconds=seconds)
        self.store.set_rate_state(
            RateState(
                name=self.name,
                last_request_at=current,
                cooldown_until=cooldown_until,
                consecutive_429_total=previous.consecutive_429_total + 1,
                upstream_429_total=previous.upstream_429_total + 1,
                last_status=status_code,
                last_error_message=error_message or f"arXiv returned HTTP {status_code}",
            )
        )
        self.store.release_lease(self.lease_name, owner_token)
        return cooldown_until

    def after_failure(
        self,
        *,
        owner_token: str,
        status: int | None = None,
        error_message: str | None = None,
        now: datetime | None = None,
    ) -> None:
        previous = self.store.get_rate_state(self.name)
        self.store.set_rate_state(
            RateState(
                name=self.name,
                last_request_at=ensure_utc(now or utc_now()),
                cooldown_until=previous.cooldown_until,
                consecutive_429_total=previous.consecutive_429_total,
                upstream_429_total=previous.upstream_429_total,
                last_status=status,
                last_error_message=error_message,
            )
        )
        self.store.release_lease(self.lease_name, owner_token)
