from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

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


def test_cooldown_persisted_during_lease_acquisition_blocks_request(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The durable state must be read after, not before, winning the lease."""
    current = datetime(2026, 1, 1, tzinfo=UTC)
    cooldown = current + timedelta(minutes=5)
    acquire = store.acquire_lease
    injected = False

    def acquire_with_racing_cooldown(
        name: str,
        owner_token: str,
        timeout_seconds: int,
        *,
        now: datetime | None = None,
    ) -> bool:
        nonlocal injected
        result = acquire(name, owner_token, timeout_seconds, now=now)
        if result and not injected:
            injected = True
            store.set_rate_state(RateState(cooldown_until=cooldown))
        return result

    monkeypatch.setattr(store, "acquire_lease", acquire_with_racing_cooldown)

    decision = HuldraRateLimiter(store, settings).before_request(
        owner_token="w1",
        now=current,
    )

    assert not decision.can_fetch
    assert decision.blocked_reason == "cooldown"
    assert decision.cooldown_until == cooldown


def test_rate_limiter_does_not_forward_prelock_time_to_upstream_lease(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    acquire = store.acquire_lease
    renew = store.renew_lease_if_owned
    observed_acquire_now: list[datetime | None] = []
    observed_renew_now: list[datetime | None] = []

    def capture_acquire_time(
        name: str,
        owner_token: str,
        timeout_seconds: int,
        *,
        now: datetime | None = None,
    ) -> bool:
        observed_acquire_now.append(now)
        return acquire(name, owner_token, timeout_seconds, now=now)

    def capture_renew_time(
        name: str,
        owner_token: str,
        timeout_seconds: int,
        *,
        now: datetime | None = None,
    ) -> bool:
        observed_renew_now.append(now)
        return renew(name, owner_token, timeout_seconds, now=now)

    monkeypatch.setattr(store, "acquire_lease", capture_acquire_time)
    monkeypatch.setattr(store, "renew_lease_if_owned", capture_renew_time)

    decision = HuldraRateLimiter(store, settings).before_request(
        owner_token="legacy:worker"
    )

    assert decision.can_fetch
    assert observed_acquire_now == [None]
    assert observed_renew_now == [None]


def test_rate_limiter_lease_covers_sqlite_wait_and_post_fetch_persistence(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tuned = settings.model_copy(update={"lease_timeout_seconds": 1})
    observed_timeouts: list[int] = []
    renew = store.renew_lease_if_owned

    def capture_renew_timeout(
        name: str,
        owner_token: str,
        timeout_seconds: int,
        *,
        now: datetime | None = None,
    ) -> bool:
        observed_timeouts.append(timeout_seconds)
        return renew(name, owner_token, timeout_seconds, now=now)

    monkeypatch.setattr(store, "renew_lease_if_owned", capture_renew_timeout)

    decision = HuldraRateLimiter(store, tuned).before_request(
        owner_token="legacy:worker"
    )

    assert decision.can_fetch
    assert observed_timeouts == [36]


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
    assert cooldown == now + timedelta(seconds=settings.cooldown_seconds)
    assert store.get_rate_state().cooldown_until == cooldown


def test_after_429_applies_safety_floor_to_zero_retry_after(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    """Retry-After: 0 cannot disable Huldra's safety cooldown."""
    limiter = HuldraRateLimiter(store, settings)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    assert limiter.before_request(owner_token="w1", now=now).can_fetch

    cooldown = limiter.after_429(owner_token="w1", retry_after_seconds=0, now=now)

    assert cooldown == now + timedelta(seconds=settings.cooldown_seconds)
    assert store.get_rate_state().cooldown_until == cooldown


def test_retry_after_is_a_hard_lower_bound_and_jitter_only_moves_upward(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    tuned = settings.model_copy(
        update={
            "rate_limit_jitter_seconds": 30.0,
            "rate_limit_max_cooldown_seconds": 100.0,
        }
    )
    limiter = HuldraRateLimiter(store, tuned, jitter=lambda maximum: maximum / 2)
    now = datetime(2026, 1, 1, tzinfo=UTC)

    cooldown = limiter.after_429(owner_token="w1", retry_after_seconds=120, now=now)

    assert cooldown == now + timedelta(seconds=135)


def test_consecutive_rate_limits_adapt_to_a_cap_and_success_recovers(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    tuned = settings.model_copy(
        update={
            "cooldown_seconds": 10,
            "rate_limit_backoff_multiplier": 2.0,
            "rate_limit_max_cooldown_seconds": 25,
        }
    )
    limiter = HuldraRateLimiter(store, tuned)
    now = datetime(2026, 1, 1, tzinfo=UTC)

    first = limiter.after_429(owner_token="w1", retry_after_seconds=0, now=now)
    second = limiter.after_429(owner_token="w1", retry_after_seconds=0, now=now)
    third = limiter.after_429(owner_token="w1", retry_after_seconds=0, now=now)

    assert first == now + timedelta(seconds=10)
    assert second == now + timedelta(seconds=20)
    assert third == now + timedelta(seconds=25)
    assert store.get_rate_state().consecutive_rate_limit_total == 3

    limiter.after_success(owner_token="w1", now=now + timedelta(seconds=26))
    recovered = limiter.after_429(
        owner_token="w1",
        retry_after_seconds=0,
        now=now + timedelta(seconds=27),
    )

    assert recovered == now + timedelta(seconds=37)
    assert store.get_rate_state().consecutive_rate_limit_total == 1


def test_rate_limit_blocks_until_the_exact_effective_cooldown_boundary(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    limiter = HuldraRateLimiter(store, settings)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    cooldown = limiter.after_429(owner_token="w1", retry_after_seconds=0, now=now)

    blocked = limiter.before_request(
        owner_token="w2",
        now=cooldown - timedelta(microseconds=1),
    )
    allowed = limiter.before_request(owner_token="w2", now=cooldown)

    assert not blocked.can_fetch
    assert blocked.cooldown_until == cooldown
    assert allowed.can_fetch


def test_oai_503_retry_after_is_not_counted_as_an_upstream_429(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    HuldraRateLimiter(store, settings).after_429(
        owner_token="oai:job",
        retry_after_seconds=42,
        status_code=503,
        now=now,
    )

    status = store.status_summary(now=now)
    assert status.upstream_rate_limited_total == 1
    assert status.upstream_429_total == 0
    assert status.upstream_oai_503_retry_after_total == 1


def test_limiter_records_bounded_request_timing_and_rate_limit_policy(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    store.set_rate_state(RateState(last_request_at=now))
    limiter = HuldraRateLimiter(store, settings)

    decision = limiter.before_request(owner_token="oai:job", now=now + timedelta(seconds=1))
    limiter.after_429(
        owner_token="oai:job",
        retry_after_seconds=0,
        status_code=503,
        now=now + timedelta(seconds=3, milliseconds=250),
    )

    assert decision.wait_seconds == 2
    events = [event for event in store.events() if event["event_type"].startswith("upstream_request_")]
    assert [event["event_type"] for event in events] == [
        "upstream_request_started",
        "upstream_request_finished",
    ]
    started = events[0]["payload"]
    assert started == {
        "api_family": "oai_pmh",
        "rate_wait_seconds": 2.0,
        "request_started_at": "2026-01-01T00:00:03+00:00",
    }
    finished = events[1]["payload"]
    assert finished["api_family"] == "oai_pmh"
    assert finished["outcome"] == "rate_limited"
    assert finished["rate_limit_kind"] == "oai_503_retry_after"
    assert finished["status"] == 503
    assert finished["request_started_at"] == "2026-01-01T00:00:03+00:00"
    assert finished["rate_wait_seconds"] == 2.0
    assert finished["latency_ms"] == 250.0
    assert finished["retry_after_seconds"] == 0
    assert finished["effective_cooldown_seconds"] == settings.cooldown_seconds
    assert finished["consecutive_rate_limit_total"] == 1
    assert finished["cooldown_until"] == "2026-01-01T00:01:03.250000+00:00"
    assert not ({"owner_token", "cache_key", "query", "url"} & finished.keys())
    status = store.status_summary(now=now + timedelta(seconds=3, milliseconds=250))
    assert status.last_request_started_at == now + timedelta(seconds=3)
    assert status.last_rate_wait_seconds == 2
    assert status.last_request_latency_ms == 250
    assert status.last_retry_after_seconds == 0
    assert status.last_effective_cooldown_seconds == settings.cooldown_seconds
    assert status.last_rate_limit_kind == "oai_503_retry_after"
    assert status.last_api_family == "oai_pmh"


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


def test_lease_timeout_covers_rate_wait_request_and_db_persistence(
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
        now=now + timedelta(seconds=75),
    ).can_fetch
