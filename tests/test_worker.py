from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from huldra.broker import HuldraBroker
from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.fetcher import ArxivApiFetcher, FetchResult, RateLimitedError, TransientFetchError
from huldra.keys import request_cache_key
from huldra.limiter import HuldraRateLimiter
from huldra.models import ArxivRequest, CachePolicy, QueueWorkKind
from huldra.time import utc_now
from huldra.worker import HuldraWorker
from tests.conftest import make_paper

ERROR_FEED = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>http://arxiv.org/api/errors</id>
    <link href="http://arxiv.org/api/errors" rel="alternate"/>
    <title>Error</title>
    <summary>incorrect id format</summary>
    <author><name>arXiv api core</name></author>
  </entry>
</feed>"""


@dataclass
class FakeFetcher:
    responses: list[object]
    calls: int = 0

    def fetch(self, request: ArxivRequest) -> FetchResult:
        self.calls += 1
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response  # type: ignore[return-value]


def _advance_clock_after_write_lock(
    store: HuldraStore,
    monkeypatch: pytest.MonkeyPatch,
    clock: list[datetime],
    *,
    seconds: float,
) -> None:
    original_begin = store.begin_immediate

    @contextmanager
    def delayed_begin() -> Iterator[sqlite3.Connection]:
        with original_begin() as conn:
            clock[0] += timedelta(seconds=seconds)
            yield conn

    monkeypatch.setattr(store, "begin_immediate", delayed_begin)
    monkeypatch.setattr("huldra.db.utc_now", lambda: clock[0])


def test_lease_ttl_starts_after_sqlite_write_lock(
    store: HuldraStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
    clock = [started]
    _advance_clock_after_write_lock(store, monkeypatch, clock, seconds=5)

    acquired = store.acquire_lease("test-lease", "worker", timeout_seconds=3)

    assert acquired
    with store.connect() as conn:
        row = conn.execute(
            "SELECT acquired_at, expires_at FROM leases WHERE name='test-lease'"
        ).fetchone()
    assert row is not None
    assert datetime.fromisoformat(row["acquired_at"]) == started + timedelta(seconds=5)
    assert datetime.fromisoformat(row["expires_at"]) == started + timedelta(seconds=8)


def test_queue_claim_ttl_starts_after_sqlite_write_lock(
    store: HuldraStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queued = store.enqueue_request(ArxivRequest(client_id="demo", search_query="cat:cs.AI"))
    started = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
    clock = [started]
    _advance_clock_after_write_lock(store, monkeypatch, clock, seconds=5)

    claimed = store.claim_next_queue_item(
        owner_token="worker",
        claim_timeout_seconds=3,
    )

    assert claimed is not None
    assert claimed.request_id == queued.request_id
    assert claimed.claimed_until == started + timedelta(seconds=8)


def test_id_fetch_reservation_ttl_starts_after_sqlite_write_lock(
    store: HuldraStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
    clock = [started]
    _advance_clock_after_write_lock(store, monkeypatch, clock, seconds=5)

    acquired = store.acquire_id_fetch_reservations(
        ["2401.00001"],
        owner_token="worker",
        request_id="request",
        ttl_seconds=3,
    )

    assert acquired.acquired_ids == ("2401.00001",)
    with store.connect() as conn:
        row = conn.execute(
            "SELECT acquired_at, expires_at FROM id_fetch_reservations "
            "WHERE arxiv_id='2401.00001'"
        ).fetchone()
    assert row is not None
    assert datetime.fromisoformat(row["acquired_at"]) == started + timedelta(seconds=5)
    assert datetime.fromisoformat(row["expires_at"]) == started + timedelta(seconds=8)


def test_two_workers_cannot_claim_same_item(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    store.enqueue_request(ArxivRequest(client_id="demo", search_query="cat:cs.AI"))
    first = store.claim_next_queue_item(owner_token="w1")
    second = store.claim_next_queue_item(owner_token="w2")
    assert first is not None
    assert second is None


def test_worker_successfully_processes_queued_item(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI")
    store.enqueue_request(request)
    fetcher = FakeFetcher([FetchResult([make_paper()], total_results=1)])
    result = HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()
    assert result.status == "completed"
    assert fetcher.calls == 1
    assert store.get_cache_entry(result.cache_key or "") is not None
    assert store.status_summary().papers_total == 1


def test_worker_rechecks_claim_and_cache_after_rate_wait(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
    clock = [started]

    def now() -> datetime:
        return clock[0]

    monkeypatch.setattr("huldra.db.utc_now", now)
    monkeypatch.setattr("huldra.limiter.utc_now", now)
    monkeypatch.setattr("huldra.worker.utc_now", now)
    tuned = settings.model_copy(update={"queue_claim_timeout_seconds": 1})
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI", max_results=1)
    queued = store.enqueue_request(request)
    store.set_rate_state(store.get_rate_state().model_copy(update={"last_request_at": started}))
    second_store = HuldraStore(settings.db_path)

    def complete_from_second_worker(seconds: float) -> None:
        clock[0] = started + timedelta(seconds=2)
        reclaimed = second_store.claim_next_queue_item(
            owner_token="legacy:second-worker",
            claim_timeout_seconds=1,
        )
        assert reclaimed is not None
        assert reclaimed.request_id == queued.request_id
        second_store.record_completed_cache_entry(
            cache_key=queued.cache_key,
            request=request,
            papers=[make_paper()],
            total_results=1,
        )
        second_store.complete_queue_item(queued.request_id)
        clock[0] = started + timedelta(seconds=seconds)

    fetcher = FakeFetcher([FetchResult([make_paper()], total_results=1)])

    result = HuldraWorker(
        store,
        tuned,
        fetcher=fetcher,
        owner_token="legacy:first-worker",
        sleep=complete_from_second_worker,
    ).run_once()

    assert result.status == "lost_claim"
    assert fetcher.calls == 0
    entry = store.get_cache_entry(queued.cache_key)
    assert entry is not None
    assert entry.upstream_requests_total == 1


def test_worker_revalidates_missing_ids_after_rate_wait(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
    clock = [started]

    def now() -> datetime:
        return clock[0]

    monkeypatch.setattr("huldra.db.utc_now", now)
    monkeypatch.setattr("huldra.limiter.utc_now", now)
    monkeypatch.setattr("huldra.worker.utc_now", now)
    tuned = settings.model_copy(update={"queue_claim_timeout_seconds": 1})
    request = ArxivRequest(
        client_id="demo",
        id_list=("2401.00001", "2401.00002"),
        max_results=2,
    )
    store.enqueue_request(request)
    store.set_rate_state(store.get_rate_state().model_copy(update={"last_request_at": started}))

    def cache_first_id_during_wait(seconds: float) -> None:
        clock[0] = started + timedelta(seconds=2)
        store.upsert_papers([make_paper("2401.00001v1")])
        clock[0] = started + timedelta(seconds=seconds)

    class IdAwareFetcher:
        def __init__(self) -> None:
            self.batches: list[tuple[str, ...]] = []

        def fetch(self, request: ArxivRequest) -> FetchResult:
            batch = tuple(request.id_list)
            self.batches.append(batch)
            return FetchResult(
                [make_paper(f"{arxiv_id}v1") for arxiv_id in batch],
                total_results=len(batch),
            )

    fetcher = IdAwareFetcher()

    result = HuldraWorker(
        store,
        tuned,
        fetcher=fetcher,
        owner_token="legacy:id-worker",
        sleep=cache_first_id_during_wait,
    ).run_once()

    assert result.status == "completed"
    assert fetcher.batches == [("2401.00002",)]
    entry = store.get_cache_entry(request_cache_key(request))
    assert entry is not None
    assert entry.result_count == 2


def test_budget_db_wait_cannot_expire_claim_and_upstream_lease_before_fetch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
    clock = [started]
    tuned = HuldraSettings(
        db_path=tmp_path / "budget-db-wait-lifecycle.db",
        request_interval_seconds=3.0,
        cooldown_seconds=60,
        rate_limit_jitter_seconds=0.0,
        worker_poll_interval_seconds=1.0,
        request_timeout_seconds=0.2,
        lease_timeout_seconds=3,
        queue_claim_timeout_seconds=1,
    )
    monkeypatch.setattr("huldra.db.utc_now", lambda: clock[0])
    monkeypatch.setattr("huldra.limiter.utc_now", lambda: clock[0])
    monkeypatch.setattr("huldra.worker.utc_now", lambda: clock[0])

    seed = HuldraStore(tuned.db_path)
    seed.init_schema()
    budget_id = seed.create_upstream_request_budget(max_requests=2, deadline_at=None)
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI", max_results=1)
    queued, joined = seed.enqueue_request_for_work(
        request,
        upstream_budget_id=budget_id,
    )
    assert not joined

    reserve_stage = threading.Event()
    blocker_ready = threading.Event()
    lock_attempted = threading.Event()
    reserve_committed = threading.Event()
    allow_first_to_continue = threading.Event()
    reserve_active = threading.Event()
    errors: list[BaseException] = []
    first_results: list[object] = []

    @dataclass
    class CountingFetcher:
        calls: int = 0

        def fetch(self, request: ArxivRequest) -> FetchResult:
            self.calls += 1
            return FetchResult([make_paper()], total_results=1)

    fetcher = CountingFetcher()
    first_store = HuldraStore(tuned.db_path, timeout=5)
    second_store = HuldraStore(tuned.db_path, timeout=5)
    original_begin = first_store.begin_immediate
    original_reserve = first_store.reserve_upstream_request

    @contextmanager
    def signal_budget_lock_attempt() -> Iterator[sqlite3.Connection]:
        if reserve_active.is_set():
            lock_attempted.set()
        with original_begin() as conn:
            yield conn

    def blocked_reserve(
        budget: str,
        *,
        now: datetime | None = None,
    ) -> str | None:
        reserve_stage.set()
        assert blocker_ready.wait(timeout=5)
        reserve_active.set()
        try:
            outcome = original_reserve(budget, now=now)
        finally:
            reserve_active.clear()
        reserve_committed.set()
        assert allow_first_to_continue.wait(timeout=5)
        return outcome

    monkeypatch.setattr(first_store, "begin_immediate", signal_budget_lock_attempt)
    monkeypatch.setattr(first_store, "reserve_upstream_request", blocked_reserve)

    def advance_clock(seconds: float) -> None:
        clock[0] += timedelta(seconds=seconds)

    first_worker = HuldraWorker(
        first_store,
        tuned,
        fetcher=fetcher,
        limiter=HuldraRateLimiter(first_store, tuned),
        owner_token="legacy:first-budget-worker",
        sleep=advance_clock,
        name="first-budget-worker",
    )
    second_worker = HuldraWorker(
        second_store,
        tuned,
        fetcher=fetcher,
        limiter=HuldraRateLimiter(second_store, tuned),
        owner_token="legacy:second-budget-worker",
        sleep=advance_clock,
        name="second-budget-worker",
    )

    def run_first() -> None:
        try:
            first_results.append(first_worker.run_once(target_cache_keys={queued.cache_key}))
        except BaseException as exc:
            errors.append(exc)

    first_thread = threading.Thread(target=run_first, daemon=True)
    first_thread.start()
    assert reserve_stage.wait(timeout=2)

    blocker = sqlite3.connect(tuned.db_path, timeout=5)
    blocker.execute("BEGIN IMMEDIATE")
    blocker_ready.set()
    assert lock_attempted.wait(timeout=2)
    # The budget write wait outlives both the renewed claim and upstream lease.
    clock[0] += timedelta(seconds=8)
    blocker.commit()
    blocker.close()
    assert reserve_committed.wait(timeout=2)

    second_result = second_worker.run_once(target_cache_keys={queued.cache_key})
    allow_first_to_continue.set()
    first_thread.join(timeout=5)

    assert not errors
    assert not first_thread.is_alive()
    assert second_result.request_id in {None, queued.request_id}
    # Reserving the durable budget conservatively is acceptable; duplicate I/O is not.
    assert fetcher.calls <= 1


def test_worker_records_atom_error_feed_failure_without_api_errors_paper(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = ArxivRequest(client_id="demo", id_list=("bad id",))
    store.enqueue_request(request)
    fetcher = ArxivApiFetcher(
        settings,
        client=httpx.Client(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, text=ERROR_FEED))
        ),
    )

    result = HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()
    entry = store.get_cache_entry(result.cache_key or "")

    assert result.status == "failed"
    assert entry is not None
    assert entry.status == "failed"
    assert entry.error_category == "non_retryable"
    assert store.get_paper("api/errors") is None
    assert store.status_summary().papers_total == 0


def test_worker_429_persists_cooldown_and_does_not_continue(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    store.enqueue_request(ArxivRequest(client_id="demo", search_query="cat:cs.AI"))
    store.enqueue_request(ArxivRequest(client_id="demo", search_query="cat:cs.LG"))
    fetcher = FakeFetcher([RateLimitedError(30)])
    result = HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()
    assert result.status == "rate_limited"
    assert fetcher.calls == 1
    assert store.get_rate_state().cooldown_until is not None
    status = store.status_summary()
    assert status.queue_depth_total == 2
    assert status.worker_last_heartbeat_at is not None
    assert status.worker_next_wake_at == result.cooldown_until
    assert status.worker_last_error_category == "rate_limited"


def test_refresh_429_preserves_old_completed_cache(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = ArxivRequest(
        client_id="demo",
        search_query="cat:cs.AI",
        cache_policy=CachePolicy.STALE_WHILE_REVALIDATE,
    )
    key = request_cache_key(request)
    store.record_completed_cache_entry(
        cache_key=key,
        request=request,
        papers=[make_paper("2401.00001v1")],
    )
    store.enqueue_request(request, key)
    fetcher = FakeFetcher([RateLimitedError(30)])
    HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()
    entry = store.get_cache_entry(key)
    assert entry is not None
    assert entry.status == "completed"
    assert store.get_cached_papers(key)[0].arxiv_id == "2401.00001v1"
    assert fetcher.calls == 1


def test_stale_request_records_durable_refresh_work_kind(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = ArxivRequest(
        client_id="demo",
        search_query="cat:cs.AI",
        cache_policy=CachePolicy.STALE_WHILE_REVALIDATE,
    )
    key = request_cache_key(request)
    store.record_completed_cache_entry(
        cache_key=key,
        request=request,
        papers=[make_paper()],
        completed_at=utc_now() - timedelta(hours=2),
    )

    result = HuldraBroker(store=store, settings=settings).ensure(request)

    assert result.stale
    assert result.request_id is not None
    item = store.get_queue_item(result.request_id)
    assert item is not None
    assert item.work_kind == QueueWorkKind.REFRESH_COMPLETED


def test_swr_refreshes_once_only_after_persisted_refresh_deadline(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    now = utc_now()
    request = ArxivRequest(
        client_id="demo",
        search_query="cat:cs.AI",
        cache_policy=CachePolicy.STALE_WHILE_REVALIDATE,
        refresh_interval_seconds=3600,
    )
    key = request_cache_key(request)
    store.record_completed_cache_entry(
        cache_key=key,
        request=request,
        papers=[make_paper()],
        completed_at=now,
    )

    fresh = HuldraBroker(store=store, settings=settings).ensure(request)

    assert not fresh.stale
    assert fresh.request_id is None
    assert store.status_summary().queue_depth_total == 0
    entry = store.get_cache_entry(key)
    assert entry is not None
    assert entry.refresh_after == now + timedelta(seconds=3600)

    store.record_completed_cache_entry(
        cache_key=key,
        request=request,
        papers=[make_paper()],
        completed_at=now - timedelta(hours=2),
    )
    first_due = HuldraBroker(store=store, settings=settings).ensure(request)
    second_due = HuldraBroker(store=store, settings=settings).ensure(request)

    assert first_due.stale
    assert first_due.request_id is not None
    assert second_due.request_id == first_due.request_id
    assert store.status_summary().queue_depth_total == 1
    reserved = store.get_cache_entry(key)
    assert reserved is not None
    assert reserved.refresh_after is not None
    assert reserved.refresh_after > now


def test_swr_caller_can_request_a_shorter_refresh_interval(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    completed_at = utc_now() - timedelta(minutes=2)
    cached_request = ArxivRequest(
        client_id="writer",
        search_query="cat:cs.AI",
        refresh_interval_seconds=3600,
    )
    key = request_cache_key(cached_request)
    store.record_completed_cache_entry(
        cache_key=key,
        request=cached_request,
        papers=[make_paper()],
        completed_at=completed_at,
    )
    refresh_request = cached_request.model_copy(
        update={
            "client_id": "reader",
            "cache_policy": CachePolicy.STALE_WHILE_REVALIDATE,
            "refresh_interval_seconds": 60,
        }
    )

    result = HuldraBroker(store=store, settings=settings).ensure(refresh_request)

    assert result.stale
    assert result.request_id is not None


def test_refresh_work_fetches_even_when_completed_cache_exists(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = ArxivRequest(
        client_id="demo",
        search_query="cat:cs.AI",
        cache_policy=CachePolicy.STALE_WHILE_REVALIDATE,
    )
    key = request_cache_key(request)
    store.record_completed_cache_entry(cache_key=key, request=request, papers=[make_paper("2401.00001v1")])
    store.enqueue_request(request, key)
    fetcher = FakeFetcher([FetchResult([make_paper("2401.00002v1")], total_results=1)])

    result = HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()

    assert result.status == "completed"
    assert fetcher.calls == 1
    assert store.get_cached_papers(key)[0].arxiv_id == "2401.00002v1"


def test_normal_pending_item_is_promoted_to_refresh_work(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    base = ArxivRequest(client_id="demo", search_query="cat:cs.AI")
    key = request_cache_key(base)
    normal = store.enqueue_request(base, key)
    stale = base.model_copy(update={"cache_policy": CachePolicy.STALE_WHILE_REVALIDATE})

    refreshed = store.enqueue_request(stale, key)

    assert refreshed.request_id == normal.request_id
    item = store.get_queue_item(normal.request_id)
    assert item is not None
    assert item.work_kind == QueueWorkKind.REFRESH_COMPLETED


def test_worker_recovers_stale_claim(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    item = store.enqueue_request(ArxivRequest(client_id="demo", search_query="cat:cs.AI"))
    assert store.claim_next_queue_item(owner_token="w1", claim_timeout_seconds=1)
    recovered = store.claim_next_queue_item(
        owner_token="w2",
        claim_timeout_seconds=1,
        now=utc_now() + timedelta(seconds=2),
    )
    assert recovered is not None
    assert recovered.request_id == item.request_id
    assert recovered.claimed_by == "w2"


def test_worker_cooldown_block_records_next_wake_without_fetch(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    store.enqueue_request(ArxivRequest(client_id="demo", search_query="cat:cs.AI"))
    fetcher = FakeFetcher([])
    cooldown_until = utc_now() + timedelta(seconds=30)
    store.set_rate_state(
        store.get_rate_state().model_copy(update={"cooldown_until": cooldown_until})
    )

    result = HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()

    assert result.status == "cooling_down"
    assert fetcher.calls == 0
    status = store.status_summary()
    assert status.worker_last_heartbeat_at is not None
    assert status.worker_next_wake_at == cooldown_until
    assert status.worker_last_error_category == "cooldown"


def test_worker_transient_failure_records_error_diagnostics(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    store.enqueue_request(ArxivRequest(client_id="demo", search_query="cat:cs.AI"))
    fetcher = FakeFetcher([TransientFetchError("temporary outage", status_code=503)])

    result = HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()

    assert result.status == "transient_failure"
    assert fetcher.calls == 1
    status = store.status_summary()
    assert status.worker_last_heartbeat_at is not None
    assert status.worker_next_wake_at is not None
    assert status.worker_last_error_category == "transient"
    assert "temporary outage" in (status.worker_last_error_message or "")


def test_worker_transient_failure_returns_retry_wake_time(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    """Regression: CLI loop needs the transient retry wake time to avoid idle polling."""
    store.enqueue_request(ArxivRequest(client_id="demo", search_query="cat:cs.AI"))
    fetcher = FakeFetcher([TransientFetchError("temporary outage", status_code=503)])

    result = HuldraWorker(store, settings, fetcher=fetcher, sleep=lambda _: None).run_once()

    assert result.status == "transient_failure"
    assert result.cooldown_until is not None
    status = store.status_summary()
    assert result.cooldown_until == status.worker_next_wake_at
    assert result.request_id is not None
    queued = store.get_queue_item(result.request_id)
    assert queued is not None
    assert queued.next_attempt_at == result.cooldown_until


def test_worker_cache_hit_updates_completion_state_without_pass_events(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI")
    key = request_cache_key(request)
    store.record_completed_cache_entry(
        cache_key=key,
        request=request,
        papers=[make_paper()],
    )
    store.enqueue_request(request, key)
    events_before = store.status_summary().events_total

    result = HuldraWorker(
        store,
        settings,
        fetcher=FakeFetcher([]),
        sleep=lambda _: None,
    ).run_once()

    assert result.status == "cache_hit"
    assert store.status_summary().worker_last_heartbeat_at is not None
    assert store.status_summary().events_total == events_before


def test_idle_worker_pass_updates_state_without_persisting_pass_events(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    events_before = store.status_summary().events_total

    result = HuldraWorker(store, settings, fetcher=FakeFetcher([])).run_once()

    assert result.status == "idle"
    status = store.status_summary()
    assert status.worker_last_heartbeat_at is not None
    assert status.worker_next_wake_at is not None
    assert status.events_total == events_before
