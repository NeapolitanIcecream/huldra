from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from typer.testing import CliRunner

import huldra.cli as cli
from huldra.db import HuldraStore
from huldra.models import ArxivRequest, CoverageStatus, LegacySyncMode, QueueItem, RequestStatus
from huldra.time import isoformat_or_none, utc_now


def _age_event(store: HuldraStore, event_type: str, *, timestamp: str) -> None:
    with store.begin_immediate() as conn:
        conn.execute(
            "UPDATE events SET created_at = ? WHERE event_type = ?",
            (timestamp, event_type),
        )


def _age_sync_job(store: HuldraStore, sync_job_id: str, *, timestamp: str) -> None:
    with store.begin_immediate() as conn:
        conn.execute(
            "UPDATE sync_jobs SET created_at = ?, updated_at = ?, completed_at = ? "
            "WHERE sync_job_id = ?",
            (timestamp, timestamp, timestamp, sync_job_id),
        )


def _queue_request(store: HuldraStore, suffix: str) -> QueueItem:
    return store.enqueue_request(ArxivRequest(client_id="gc-test", search_query=f"cat:{suffix}"))


def _finish_queue_work(
    store: HuldraStore,
    item: QueueItem,
    request: ArxivRequest,
    *,
    status: RequestStatus,
    timestamp: datetime,
) -> None:
    if status == RequestStatus.COMPLETED:
        store.record_completed_cache_entry(
            cache_key=item.cache_key,
            request=request,
            papers=[],
            total_results=0,
            requested_at=timestamp,
            completed_at=timestamp,
        )
        store.complete_queue_item(item.request_id, now=timestamp)
        return
    store.record_cache_failure(
        cache_key=item.cache_key,
        request=request,
        error_category="non_retryable",
        error_message="terminal test failure",
        requested_at=timestamp,
    )
    store.release_or_delay_queue_item(
        item.request_id,
        status=RequestStatus.FAILED,
        error_category="non_retryable",
        error_message="terminal test failure",
        now=timestamp,
    )


def _sqlite_storage_bytes(db: Path) -> int:
    return sum(
        path.stat().st_size
        for path in (db, Path(f"{db}-wal"), Path(f"{db}-shm"))
        if path.exists()
    )


def test_retention_gc_is_dry_run_first_and_preserves_active_state(store: HuldraStore) -> None:
    now = utc_now()
    cutoff = now - timedelta(days=30)
    old = isoformat_or_none(now - timedelta(days=60))
    assert old is not None

    store.record_event("old_event", {})
    store.record_event("new_event", {})
    _age_event(store, "old_event", timestamp=old)

    completed = _queue_request(store, "completed")
    store.complete_queue_item(completed.request_id, now=now - timedelta(days=60))
    failed = _queue_request(store, "failed")
    store.release_or_delay_queue_item(
        failed.request_id,
        status=RequestStatus.FAILED,
        now=now - timedelta(days=60),
    )
    queued = _queue_request(store, "queued")
    delayed = _queue_request(store, "delayed")
    store.release_or_delay_queue_item(
        delayed.request_id,
        next_attempt_at=now + timedelta(days=1),
        now=now - timedelta(days=60),
    )
    claimed = _queue_request(store, "claimed")
    with store.begin_immediate() as conn:
        conn.execute(
            "UPDATE queue_items SET created_at = ?, updated_at = ? "
            "WHERE request_id IN (?, ?)",
            (old, old, queued.request_id, claimed.request_id),
        )
        conn.execute(
            "UPDATE queue_items SET status = 'claimed', claimed_by = 'active-worker', "
            "claimed_until = ? WHERE request_id = ?",
            (isoformat_or_none(now + timedelta(days=1)), claimed.request_id),
        )
    assert store.acquire_lease("active-lease", "owner", 3600, now=now)

    request = ArxivRequest(client_id="gc-test", search_query="cat:sync")
    terminal_job = store.create_sync_job(request, LegacySyncMode.SLICE)
    store.record_sync_job_page(
        sync_job_id=terminal_job,
        request=request,
        cache_key="terminal-cache-key",
        status="completed",
    )
    store.complete_sync_job(
        sync_job_id=terminal_job,
        status="completed",
        coverage_status=CoverageStatus.SLICE,
        result_count=0,
        total_results=0,
        pages_total=1,
        pages_completed_total=1,
    )
    _age_sync_job(store, terminal_job, timestamp=old)

    running_job = store.create_sync_job(request, LegacySyncMode.SLICE)
    store.record_sync_job_page(
        sync_job_id=running_job,
        request=request,
        cache_key="running-cache-key",
        status="running",
    )
    with store.begin_immediate() as conn:
        conn.execute(
            "UPDATE sync_jobs SET created_at = ?, updated_at = ? WHERE sync_job_id = ?",
            (old, old, running_job),
        )

    pending_job = store.create_sync_job(request, LegacySyncMode.SLICE)
    store.record_sync_job_page(
        sync_job_id=pending_job,
        request=request,
        cache_key="pending-cache-key",
        status="queued",
    )
    store.complete_sync_job(
        sync_job_id=pending_job,
        status="queued",
        coverage_status=CoverageStatus.UNKNOWN,
        result_count=0,
        total_results=None,
        pages_total=1,
        pages_completed_total=0,
    )
    _age_sync_job(store, pending_job, timestamp=old)

    preview = store.gc(cutoff=cutoff)

    assert preview.dry_run
    assert preview.events_eligible_total == 1
    assert preview.queue_items_eligible_total == 2
    assert preview.sync_jobs_eligible_total == 1
    assert preview.sync_job_pages_eligible_total == 1
    assert preview.deleted_total == 0

    applied = store.gc(cutoff=cutoff, dry_run=False)

    assert not applied.dry_run
    assert applied.events_deleted_total == 1
    assert applied.queue_items_deleted_total == 2
    assert applied.sync_jobs_deleted_total == 1
    assert applied.sync_job_pages_deleted_total == 1
    with store.connect() as conn:
        remaining_events = {
            row["event_type"] for row in conn.execute("SELECT event_type FROM events").fetchall()
        }
        remaining_queue_ids = {
            row["request_id"] for row in conn.execute("SELECT request_id FROM queue_items").fetchall()
        }
        remaining_sync_ids = {
            row["sync_job_id"] for row in conn.execute("SELECT sync_job_id FROM sync_jobs").fetchall()
        }
        remaining_page_ids = {
            row["sync_job_id"]
            for row in conn.execute("SELECT sync_job_id FROM sync_job_pages").fetchall()
        }
        leases_total = int(conn.execute("SELECT COUNT(*) FROM leases").fetchone()[0])
    assert "old_event" not in remaining_events
    assert "new_event" in remaining_events
    assert {queued.request_id, delayed.request_id, claimed.request_id} <= remaining_queue_ids
    assert completed.request_id not in remaining_queue_ids
    assert failed.request_id not in remaining_queue_ids
    assert {running_job, pending_job} <= remaining_sync_ids
    assert terminal_job not in remaining_sync_ids
    assert {running_job, pending_job} <= remaining_page_ids
    assert terminal_job not in remaining_page_ids
    assert leases_total == 1


@pytest.mark.parametrize("terminal_status", [RequestStatus.COMPLETED, RequestStatus.FAILED])
def test_retention_gc_reclaims_expired_async_job_after_associated_work_finishes(
    store: HuldraStore,
    terminal_status: RequestStatus,
) -> None:
    now = utc_now()
    cutoff = now - timedelta(days=30)
    old_at = now - timedelta(days=60)
    old = isoformat_or_none(old_at)
    assert old is not None

    request = ArxivRequest(client_id="gc-test", search_query="cat:async")
    queued = store.enqueue_request(request)
    sync_job_id = store.create_sync_job(request, LegacySyncMode.SLICE)
    store.record_sync_job_page(
        sync_job_id=sync_job_id,
        request=request,
        cache_key=queued.cache_key,
        status="queued",
    )
    store.complete_sync_job(
        sync_job_id=sync_job_id,
        status="queued",
        coverage_status=CoverageStatus.UNKNOWN,
        result_count=0,
        total_results=None,
        pages_total=1,
        pages_completed_total=0,
    )
    _age_sync_job(store, sync_job_id, timestamp=old)

    active_preview = store.gc(cutoff=cutoff)

    assert active_preview.sync_jobs_eligible_total == 0
    assert active_preview.sync_job_pages_eligible_total == 0

    _finish_queue_work(
        store,
        queued,
        request,
        status=terminal_status,
        timestamp=now,
    )

    fresh_terminal_preview = store.gc(cutoff=cutoff)

    assert fresh_terminal_preview.sync_jobs_eligible_total == 0
    assert fresh_terminal_preview.sync_job_pages_eligible_total == 0

    _finish_queue_work(
        store,
        queued,
        request,
        status=terminal_status,
        timestamp=old_at,
    )

    expired_terminal_preview = store.gc(cutoff=cutoff)

    assert expired_terminal_preview.sync_jobs_eligible_total == 1
    assert expired_terminal_preview.sync_job_pages_eligible_total == 1

    applied = store.gc(cutoff=cutoff, dry_run=False)

    assert applied.sync_jobs_deleted_total == 1
    assert applied.sync_job_pages_deleted_total == 1
    assert store.get_sync_job(sync_job_id) is None
    assert store.get_queue_item(queued.request_id) is None
    assert store.get_cache_entry(queued.cache_key) is not None


def test_store_gc_cli_defaults_to_dry_run(tmp_path: Path) -> None:
    db = tmp_path / "huldra.db"
    store = HuldraStore(db)
    store.init_schema()
    store.record_event("old_event", {})
    old = isoformat_or_none(utc_now() - timedelta(days=60))
    assert old is not None
    _age_event(store, "old_event", timestamp=old)

    result = CliRunner().invoke(
        cli.app,
        ["store", "gc", "--db", str(db), "--older-than-days", "30", "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["dry_run"] is True
    assert payload["events_eligible_total"] == 1
    assert {event["event_type"] for event in store.events()} == {"old_event"}


def test_store_gc_dry_run_does_not_create_or_initialize_a_database(tmp_path: Path) -> None:
    db = tmp_path / "missing.db"

    result = CliRunner().invoke(
        cli.app,
        ["store", "gc", "--db", str(db), "--older-than-days", "30", "--json"],
        color=False,
    )

    assert result.exit_code != 0
    assert "does not exist or is unreadable" in result.output
    assert not db.exists()


def test_store_vacuum_reclaims_space_only_when_explicitly_invoked(tmp_path: Path) -> None:
    db = tmp_path / "huldra.db"
    store = HuldraStore(db)
    store.init_schema()
    old = isoformat_or_none(utc_now() - timedelta(days=60))
    assert old is not None
    payload = json.dumps({"padding": "x" * 8192})
    with store.begin_immediate() as conn:
        conn.executemany(
            "INSERT INTO events(event_type, payload_json, created_at) VALUES ('old', ?, ?)",
            ((payload, old) for _ in range(512)),
        )
    store.gc(cutoff=utc_now() - timedelta(days=30), dry_run=False)
    storage_bytes_before = _sqlite_storage_bytes(db)

    result = CliRunner().invoke(
        cli.app,
        ["store", "vacuum", "--db", str(db), "--json"],
    )

    assert result.exit_code == 0
    summary = json.loads(result.output)
    assert summary["storage_bytes_before"] >= storage_bytes_before
    assert summary["storage_bytes_after"] < summary["storage_bytes_before"]
    assert summary["reclaimed_bytes"] > 0
