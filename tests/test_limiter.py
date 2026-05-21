from __future__ import annotations

from datetime import UTC, datetime, timedelta

from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.limiter import HuldraRateLimiter
from huldra.models import RateState


def test_limiter_instances_share_sqlite_rate_state(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    first = HuldraRateLimiter(store, settings)
    second = HuldraRateLimiter(store, settings)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    first.after_success(owner_token="missing-ok", now=now)
    assert second.seconds_until_next_request(now=now + timedelta(seconds=1)) == 2


def test_cooldown_active_does_not_acquire_lease(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    store.set_rate_state(RateState(name="arxiv_legacy_api", cooldown_until=now + timedelta(minutes=5)))
    decision = HuldraRateLimiter(store, settings).before_request(
        owner_token="w1",
        now=now,
    )
    assert not decision.can_fetch
    assert decision.blocked_reason == "cooldown"
    assert store.acquire_lease("upstream_fetch", "w2", 60, now=now)


def test_lease_blocks_second_worker_until_stale(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    limiter = HuldraRateLimiter(store, settings)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    assert limiter.before_request(owner_token="w1", now=now).can_fetch
    assert not limiter.before_request(owner_token="w2", now=now).can_fetch
    later = now + timedelta(seconds=settings.lease_timeout_seconds + 1)
    assert limiter.before_request(owner_token="w2", now=later).can_fetch


def test_after_429_persists_cooldown(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    limiter = HuldraRateLimiter(store, settings)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    assert limiter.before_request(owner_token="w1", now=now).can_fetch
    cooldown = limiter.after_429(owner_token="w1", retry_after_seconds=10, now=now)
    assert cooldown == now + timedelta(seconds=10)
    assert store.get_rate_state().cooldown_until == cooldown


def test_after_429_honors_zero_retry_after(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    """Regression: Retry-After: 0 was replaced by the default cooldown."""
    limiter = HuldraRateLimiter(store, settings)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    assert limiter.before_request(owner_token="w1", now=now).can_fetch

    cooldown = limiter.after_429(owner_token="w1", retry_after_seconds=0, now=now)

    assert cooldown == now
    assert store.get_rate_state().cooldown_until == cooldown


def test_upstream_429_total_survives_success_after_cooldown(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    limiter = HuldraRateLimiter(store, settings)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    limiter.after_429(owner_token="w1", retry_after_seconds=10, now=now)

    assert store.status_summary(now=now).upstream_429_total == 1

    limiter.after_success(owner_token="w1", now=now + timedelta(seconds=11))

    assert store.status_summary(now=now + timedelta(seconds=11)).upstream_429_total == 1
    state = store.get_rate_state()
    assert state.consecutive_429_total == 0


def test_lease_timeout_covers_rate_wait_and_request_timeout(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    tuned = settings.model_copy(
        update={
            "request_interval_seconds": 10.0,
            "request_timeout_seconds": 30.0,
            "lease_timeout_seconds": 5,
        }
    )
    store.set_rate_state(RateState(last_request_at=now))

    decision = HuldraRateLimiter(store, tuned).before_request(
        owner_token="w1",
        now=now + timedelta(seconds=1),
    )

    assert decision.can_fetch
    assert not HuldraRateLimiter(store, tuned).before_request(
        owner_token="w2",
        now=now + timedelta(seconds=30),
    ).can_fetch
    assert HuldraRateLimiter(store, tuned).before_request(
        owner_token="w2",
        now=now + timedelta(seconds=45),
    ).can_fetch
