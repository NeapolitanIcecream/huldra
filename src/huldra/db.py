from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from huldra.keys import request_cache_key
from huldra.migrations import apply_migrations
from huldra.models import (
    ArxivPaper,
    ArxivRequest,
    BrokerStatus,
    CacheEntry,
    QueueItem,
    RateState,
    RequestStatus,
)
from huldra.time import ensure_utc, from_isoformat_or_none, isoformat_or_none, utc_now


class HuldraStore:
    def __init__(self, db_path: Path | str, *, timeout: float = 30.0) -> None:
        self.db_path = Path(db_path).expanduser()
        self.timeout = timeout

    def init_schema(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            apply_migrations(conn)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=self.timeout)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def begin_immediate(self) -> Iterator[sqlite3.Connection]:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except Exception:
                conn.rollback()
                raise
            else:
                conn.commit()

    def upsert_papers(self, papers: list[ArxivPaper], *, now: datetime | None = None) -> None:
        if not papers:
            return
        timestamp = isoformat_or_none(now or utc_now())
        assert timestamp is not None
        with self.begin_immediate() as conn:
            for paper in papers:
                conn.execute(
                    """
                    INSERT INTO papers (
                        arxiv_id, version, canonical_url, title, abstract,
                        authors_json, primary_category, categories_json,
                        published_at, updated_at, comment, journal_ref, doi,
                        raw_atom_json, first_seen_at, last_seen_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(arxiv_id) DO UPDATE SET
                        version=excluded.version,
                        canonical_url=excluded.canonical_url,
                        title=excluded.title,
                        abstract=excluded.abstract,
                        authors_json=excluded.authors_json,
                        primary_category=excluded.primary_category,
                        categories_json=excluded.categories_json,
                        published_at=excluded.published_at,
                        updated_at=excluded.updated_at,
                        comment=excluded.comment,
                        journal_ref=excluded.journal_ref,
                        doi=excluded.doi,
                        raw_atom_json=excluded.raw_atom_json,
                        last_seen_at=excluded.last_seen_at
                    """,
                    (
                        paper.arxiv_id,
                        paper.version,
                        paper.canonical_url,
                        paper.title,
                        paper.abstract,
                        json.dumps(paper.authors, separators=(",", ":")),
                        paper.primary_category,
                        json.dumps(paper.categories, separators=(",", ":")),
                        isoformat_or_none(paper.published_at),
                        isoformat_or_none(paper.updated_at),
                        paper.comment,
                        paper.journal_ref,
                        paper.doi,
                        json.dumps(paper.raw_atom, sort_keys=True, separators=(",", ":")),
                        timestamp,
                        timestamp,
                    ),
                )

    def record_completed_cache_entry(
        self,
        *,
        cache_key: str,
        request: ArxivRequest,
        papers: list[ArxivPaper],
        total_results: int | None = None,
        upstream_status: int = 200,
        requested_at: datetime | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        requested = isoformat_or_none(requested_at or utc_now())
        completed = isoformat_or_none(completed_at or utc_now())
        assert requested is not None and completed is not None
        self.upsert_papers(papers, now=completed_at)
        with self.begin_immediate() as conn:
            previous = conn.execute(
                "SELECT upstream_requests_total FROM cache_entries WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
            upstream_total = int(previous["upstream_requests_total"]) + 1 if previous else 1
            conn.execute(
                """
                INSERT INTO cache_entries (
                    cache_key, request_json, api_family, status, requested_at,
                    completed_at, cooldown_until, upstream_status,
                    upstream_requests_total, result_count, total_results,
                    error_category, error_message
                )
                VALUES (?, ?, ?, 'completed', ?, ?, NULL, ?, ?, ?, ?, NULL, NULL)
                ON CONFLICT(cache_key) DO UPDATE SET
                    request_json=excluded.request_json,
                    api_family=excluded.api_family,
                    status='completed',
                    requested_at=excluded.requested_at,
                    completed_at=excluded.completed_at,
                    cooldown_until=NULL,
                    upstream_status=excluded.upstream_status,
                    upstream_requests_total=excluded.upstream_requests_total,
                    result_count=excluded.result_count,
                    total_results=excluded.total_results,
                    error_category=NULL,
                    error_message=NULL
                """,
                (
                    cache_key,
                    _request_json(request),
                    request.api_family,
                    requested,
                    completed,
                    upstream_status,
                    upstream_total,
                    len(papers),
                    total_results,
                ),
            )
            conn.execute("DELETE FROM cache_matches WHERE cache_key = ?", (cache_key,))
            for position, paper in enumerate(papers):
                conn.execute(
                    """
                    INSERT INTO cache_matches(cache_key, arxiv_id, sort_position, matched_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (cache_key, paper.arxiv_id, position, completed),
                )
            self._record_event_conn(
                conn,
                "fetch_success",
                {
                    "cache_key": cache_key,
                    "papers_total": len(papers),
                    "upstream_status": upstream_status,
                },
            )

    def get_cache_entry(self, cache_key: str) -> CacheEntry | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM cache_entries WHERE cache_key = ?", (cache_key,)).fetchone()
        return _cache_entry_from_row(row) if row else None

    def get_cached_papers(self, cache_key: str) -> list[ArxivPaper]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT p.* FROM cache_matches m
                JOIN papers p ON p.arxiv_id = m.arxiv_id
                WHERE m.cache_key = ?
                ORDER BY m.sort_position ASC
                """,
                (cache_key,),
            ).fetchall()
        return [_paper_from_row(row) for row in rows]

    def get_paper(self, arxiv_id: str) -> ArxivPaper | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM papers WHERE arxiv_id = ?", (arxiv_id,)).fetchone()
        return _paper_from_row(row) if row else None

    def record_cache_failure(
        self,
        *,
        cache_key: str,
        request: ArxivRequest,
        error_category: str,
        error_message: str,
        status: str = "failed",
        cooldown_until: datetime | None = None,
        upstream_status: int | None = None,
        requested_at: datetime | None = None,
    ) -> None:
        timestamp = isoformat_or_none(requested_at or utc_now())
        cooldown = isoformat_or_none(cooldown_until)
        assert timestamp is not None
        with self.begin_immediate() as conn:
            previous = conn.execute(
                "SELECT status, upstream_requests_total FROM cache_entries WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
            final_status = "completed" if previous and previous["status"] == "completed" else status
            upstream_total = int(previous["upstream_requests_total"]) + 1 if previous else 1
            conn.execute(
                """
                INSERT INTO cache_entries (
                    cache_key, request_json, api_family, status, requested_at,
                    completed_at, cooldown_until, upstream_status,
                    upstream_requests_total, result_count, total_results,
                    error_category, error_message
                )
                VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, 0, NULL, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    request_json=excluded.request_json,
                    api_family=excluded.api_family,
                    status=?,
                    requested_at=excluded.requested_at,
                    cooldown_until=excluded.cooldown_until,
                    upstream_status=excluded.upstream_status,
                    upstream_requests_total=excluded.upstream_requests_total,
                    error_category=excluded.error_category,
                    error_message=excluded.error_message
                """,
                (
                    cache_key,
                    _request_json(request),
                    request.api_family,
                    final_status,
                    timestamp,
                    cooldown,
                    upstream_status,
                    upstream_total,
                    error_category,
                    error_message[:1000],
                    final_status,
                ),
            )
            self._record_event_conn(
                conn,
                "fetch_429" if upstream_status == 429 else "fetch_failure",
                {
                    "cache_key": cache_key,
                    "error_category": error_category,
                    "upstream_status": upstream_status,
                    "cooldown_until": cooldown,
                },
            )

    def record_rate_limited(
        self,
        *,
        cache_key: str,
        request: ArxivRequest,
        cooldown_until: datetime,
        error_message: str = "arXiv returned HTTP 429",
    ) -> None:
        self.record_cache_failure(
            cache_key=cache_key,
            request=request,
            status="rate_limited",
            cooldown_until=cooldown_until,
            upstream_status=429,
            error_category="rate_limited",
            error_message=error_message,
        )

    def get_rate_state(self, name: str = "arxiv_legacy_api") -> RateState:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM rate_state WHERE name = ?", (name,)).fetchone()
        if row is None:
            return RateState(name=name)
        return RateState(
            name=row["name"],
            last_request_at=from_isoformat_or_none(row["last_request_at"]),
            cooldown_until=from_isoformat_or_none(row["cooldown_until"]),
            consecutive_429_total=int(row["consecutive_429_total"]),
            upstream_429_total=int(row["upstream_429_total"]),
            last_status=row["last_status"],
            last_error_message=row["last_error_message"],
        )

    def set_rate_state(self, state: RateState) -> None:
        with self.begin_immediate() as conn:
            conn.execute(
                """
                INSERT INTO rate_state(
                    name, last_request_at, cooldown_until, consecutive_429_total,
                    upstream_429_total, last_status, last_error_message
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    last_request_at=excluded.last_request_at,
                    cooldown_until=excluded.cooldown_until,
                    consecutive_429_total=excluded.consecutive_429_total,
                    upstream_429_total=excluded.upstream_429_total,
                    last_status=excluded.last_status,
                    last_error_message=excluded.last_error_message
                """,
                (
                    state.name,
                    isoformat_or_none(state.last_request_at),
                    isoformat_or_none(state.cooldown_until),
                    state.consecutive_429_total,
                    state.upstream_429_total,
                    state.last_status,
                    state.last_error_message,
                ),
            )

    def enqueue_request(self, request: ArxivRequest, cache_key: str | None = None) -> QueueItem:
        key = cache_key or request_cache_key(request)
        now = utc_now()
        now_s = isoformat_or_none(now)
        assert now_s is not None
        with self.begin_immediate() as conn:
            existing = conn.execute(
                """
                SELECT * FROM queue_items
                WHERE cache_key = ? AND status IN ('queued', 'delayed', 'claimed')
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (key,),
            ).fetchone()
            if existing is not None:
                return _queue_item_from_row(existing)
            request_id = str(uuid4())
            conn.execute(
                """
                INSERT INTO queue_items(
                    request_id, cache_key, client_id, request_json, priority, status,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, 'queued', ?, ?)
                """,
                (
                    request_id,
                    key,
                    request.client_id,
                    _request_json(request),
                    request.priority,
                    now_s,
                    now_s,
                ),
            )
            self._record_event_conn(
                conn,
                "request_enqueued",
                {
                    "request_id": request_id,
                    "cache_key": key,
                    "client_id": request.client_id,
                },
            )
            row = conn.execute("SELECT * FROM queue_items WHERE request_id = ?", (request_id,)).fetchone()
        assert row is not None
        return _queue_item_from_row(row)

    def claim_next_queue_item(
        self,
        *,
        owner_token: str,
        claim_timeout_seconds: int = 300,
        now: datetime | None = None,
    ) -> QueueItem | None:
        current = ensure_utc(now or utc_now())
        current_s = isoformat_or_none(current)
        claim_until = isoformat_or_none(current + timedelta(seconds=claim_timeout_seconds))
        assert current_s is not None and claim_until is not None
        with self.begin_immediate() as conn:
            row = conn.execute(
                """
                SELECT * FROM queue_items
                WHERE (
                    status IN ('queued', 'delayed')
                    AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                ) OR (
                    status = 'claimed'
                    AND claimed_until IS NOT NULL
                    AND claimed_until <= ?
                )
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
                """,
                (current_s, current_s),
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                """
                UPDATE queue_items
                SET status='claimed',
                    claimed_by=?,
                    claimed_until=?,
                    attempts_total=attempts_total + 1,
                    updated_at=?
                WHERE request_id=?
                """,
                (owner_token, claim_until, current_s, row["request_id"]),
            )
            updated = conn.execute(
                "SELECT * FROM queue_items WHERE request_id = ?", (row["request_id"],)
            ).fetchone()
        assert updated is not None
        return _queue_item_from_row(updated)

    def get_queue_item(self, request_id: str) -> QueueItem | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM queue_items WHERE request_id = ?", (request_id,)).fetchone()
        return _queue_item_from_row(row) if row else None

    def complete_queue_item(self, request_id: str, *, now: datetime | None = None) -> None:
        timestamp = isoformat_or_none(now or utc_now())
        assert timestamp is not None
        with self.begin_immediate() as conn:
            conn.execute(
                """
                UPDATE queue_items
                SET status='completed', completed_at=?, updated_at=?,
                    claimed_by=NULL, claimed_until=NULL, error_category=NULL, error_message=NULL
                WHERE request_id=?
                """,
                (timestamp, timestamp, request_id),
            )

    def release_or_delay_queue_item(
        self,
        request_id: str,
        *,
        next_attempt_at: datetime | None = None,
        error_category: str | None = None,
        error_message: str | None = None,
        status: RequestStatus = RequestStatus.DELAYED,
        now: datetime | None = None,
    ) -> None:
        timestamp = isoformat_or_none(now or utc_now())
        next_attempt = isoformat_or_none(next_attempt_at)
        assert timestamp is not None
        with self.begin_immediate() as conn:
            conn.execute(
                """
                UPDATE queue_items
                SET status=?, next_attempt_at=?, updated_at=?,
                    claimed_by=NULL, claimed_until=NULL,
                    error_category=?, error_message=?
                WHERE request_id=?
                """,
                (
                    status,
                    next_attempt,
                    timestamp,
                    error_category,
                    error_message[:1000] if error_message else None,
                    request_id,
                ),
            )

    def acquire_lease(
        self,
        name: str,
        owner_token: str,
        timeout_seconds: int,
        *,
        now: datetime | None = None,
    ) -> bool:
        current = ensure_utc(now or utc_now())
        current_s = isoformat_or_none(current)
        expires_s = isoformat_or_none(current + timedelta(seconds=timeout_seconds))
        assert current_s is not None and expires_s is not None
        with self.begin_immediate() as conn:
            existing = conn.execute("SELECT * FROM leases WHERE name = ?", (name,)).fetchone()
            if existing is not None:
                expires_at = from_isoformat_or_none(existing["expires_at"])
                if existing["owner_token"] != owner_token and expires_at is not None and expires_at > current:
                    return False
            conn.execute(
                """
                INSERT INTO leases(name, owner_token, acquired_at, expires_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    owner_token=excluded.owner_token,
                    acquired_at=excluded.acquired_at,
                    expires_at=excluded.expires_at
                """,
                (name, owner_token, current_s, expires_s),
            )
        return True

    def release_lease(self, name: str, owner_token: str) -> None:
        with self.begin_immediate() as conn:
            conn.execute(
                "DELETE FROM leases WHERE name = ? AND owner_token = ?",
                (name, owner_token),
            )

    def record_worker_started(
        self,
        *,
        name: str = "default",
        now: datetime | None = None,
    ) -> None:
        timestamp = isoformat_or_none(now or utc_now())
        assert timestamp is not None
        with self.begin_immediate() as conn:
            conn.execute(
                """
                INSERT INTO worker_state(name, last_started_at, last_heartbeat_at)
                VALUES (?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    last_started_at=excluded.last_started_at,
                    last_heartbeat_at=excluded.last_heartbeat_at
                """,
                (name, timestamp, timestamp),
            )
            self._record_event_conn(conn, "worker_start", {"name": name})

    def record_worker_heartbeat(
        self,
        *,
        name: str = "default",
        next_wake_at: datetime | None = None,
        error_category: str | None = None,
        error_message: str | None = None,
        now: datetime | None = None,
    ) -> None:
        timestamp = isoformat_or_none(now or utc_now())
        next_wake = isoformat_or_none(next_wake_at)
        assert timestamp is not None
        with self.begin_immediate() as conn:
            conn.execute(
                """
                INSERT INTO worker_state(
                    name, last_started_at, last_heartbeat_at, next_wake_at,
                    last_error_category, last_error_message
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    last_heartbeat_at=excluded.last_heartbeat_at,
                    next_wake_at=excluded.next_wake_at,
                    last_error_category=excluded.last_error_category,
                    last_error_message=excluded.last_error_message
                """,
                (
                    name,
                    timestamp,
                    timestamp,
                    next_wake,
                    error_category,
                    error_message[:1000] if error_message else None,
                ),
            )

    def record_worker_completed(
        self,
        *,
        name: str = "default",
        next_wake_at: datetime | None = None,
        error_category: str | None = None,
        error_message: str | None = None,
        now: datetime | None = None,
    ) -> None:
        timestamp = isoformat_or_none(now or utc_now())
        next_wake = isoformat_or_none(next_wake_at)
        assert timestamp is not None
        with self.begin_immediate() as conn:
            conn.execute(
                """
                INSERT INTO worker_state(
                    name, last_completed_at, last_heartbeat_at, next_wake_at,
                    last_error_category, last_error_message
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    last_completed_at=excluded.last_completed_at,
                    last_heartbeat_at=excluded.last_heartbeat_at,
                    next_wake_at=excluded.next_wake_at,
                    last_error_category=excluded.last_error_category,
                    last_error_message=excluded.last_error_message
                """,
                (
                    name,
                    timestamp,
                    timestamp,
                    next_wake,
                    error_category,
                    error_message[:1000] if error_message else None,
                ),
            )
            self._record_event_conn(conn, "worker_stop", {"name": name})

    def status_summary(self, *, now: datetime | None = None) -> BrokerStatus:
        current = ensure_utc(now or utc_now())
        with self.connect() as conn:
            upstream = conn.execute(
                "SELECT COALESCE(SUM(upstream_requests_total), 0) AS total FROM cache_entries"
            ).fetchone()
            errors_429 = conn.execute(
                "SELECT COALESCE(SUM(upstream_429_total), 0) AS total FROM rate_state"
            ).fetchone()
            rate = conn.execute(
                "SELECT cooldown_until FROM rate_state WHERE name = 'arxiv_legacy_api'"
            ).fetchone()
            queue = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN status IN ('queued', 'claimed', 'delayed') THEN 1 ELSE 0 END) AS depth,
                    SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) AS ready,
                    SUM(CASE WHEN status = 'delayed' THEN 1 ELSE 0 END) AS delayed,
                    MIN(CASE WHEN status IN ('queued', 'claimed', 'delayed') THEN created_at END) AS oldest
                FROM queue_items
                """
            ).fetchone()
            cache = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
                    SUM(CASE WHEN status IN ('failed', 'rate_limited') THEN 1 ELSE 0 END) AS failed
                FROM cache_entries
                """
            ).fetchone()
            papers = conn.execute("SELECT COUNT(*) AS total FROM papers").fetchone()
            worker = conn.execute(
                """
                SELECT
                    last_heartbeat_at,
                    next_wake_at,
                    last_error_category,
                    last_error_message
                FROM worker_state
                ORDER BY last_heartbeat_at DESC
                LIMIT 1
                """
            ).fetchone()
        cooldown_until = from_isoformat_or_none(rate["cooldown_until"]) if rate else None
        return BrokerStatus(
            upstream_requests_total=int(upstream["total"]),
            upstream_429_total=int(errors_429["total"]),
            cooldown_until=cooldown_until,
            cooldown_active=cooldown_until is not None and cooldown_until > current,
            queue_depth_total=int(queue["depth"] or 0),
            queue_ready_total=int(queue["ready"] or 0),
            queue_delayed_total=int(queue["delayed"] or 0),
            cache_entries_total=int(cache["total"]),
            cache_completed_total=int(cache["completed"] or 0),
            cache_failed_total=int(cache["failed"] or 0),
            papers_total=int(papers["total"]),
            worker_last_heartbeat_at=(
                from_isoformat_or_none(worker["last_heartbeat_at"]) if worker else None
            ),
            worker_next_wake_at=(
                from_isoformat_or_none(worker["next_wake_at"]) if worker else None
            ),
            worker_last_error_category=(
                worker["last_error_category"] if worker else None
            ),
            worker_last_error_message=(
                worker["last_error_message"] if worker else None
            ),
            oldest_pending_request_at=from_isoformat_or_none(queue["oldest"]),
        )

    def events(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM events ORDER BY id ASC").fetchall()
        return [
            {
                "id": int(row["id"]),
                "event_type": row["event_type"],
                "payload": json.loads(row["payload_json"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def _record_event_conn(
        self,
        conn: sqlite3.Connection,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        timestamp = isoformat_or_none(utc_now())
        assert timestamp is not None
        conn.execute(
            "INSERT INTO events(event_type, payload_json, created_at) VALUES (?, ?, ?)",
            (
                event_type,
                json.dumps(payload, sort_keys=True, separators=(",", ":")),
                timestamp,
            ),
        )


def _request_json(request: ArxivRequest) -> str:
    return request.model_dump_json()


def _request_from_json(value: str) -> ArxivRequest:
    return ArxivRequest.model_validate_json(value)


def _cache_entry_from_row(row: sqlite3.Row) -> CacheEntry:
    return CacheEntry(
        cache_key=row["cache_key"],
        request=_request_from_json(row["request_json"]),
        api_family=row["api_family"],
        status=row["status"],
        requested_at=from_isoformat_or_none(row["requested_at"]),
        completed_at=from_isoformat_or_none(row["completed_at"]),
        cooldown_until=from_isoformat_or_none(row["cooldown_until"]),
        upstream_status=row["upstream_status"],
        upstream_requests_total=int(row["upstream_requests_total"]),
        result_count=int(row["result_count"]),
        total_results=row["total_results"],
        error_category=row["error_category"],
        error_message=row["error_message"],
    )


def _queue_item_from_row(row: sqlite3.Row) -> QueueItem:
    return QueueItem(
        request_id=row["request_id"],
        cache_key=row["cache_key"],
        client_id=row["client_id"],
        request=_request_from_json(row["request_json"]),
        priority=int(row["priority"]),
        status=row["status"],
        created_at=from_isoformat_or_none(row["created_at"]) or utc_now(),
        updated_at=from_isoformat_or_none(row["updated_at"]) or utc_now(),
        claimed_by=row["claimed_by"],
        claimed_until=from_isoformat_or_none(row["claimed_until"]),
        attempts_total=int(row["attempts_total"]),
        next_attempt_at=from_isoformat_or_none(row["next_attempt_at"]),
        completed_at=from_isoformat_or_none(row["completed_at"]),
        error_category=row["error_category"],
        error_message=row["error_message"],
    )


def _paper_from_row(row: sqlite3.Row) -> ArxivPaper:
    return ArxivPaper(
        arxiv_id=row["arxiv_id"],
        version=row["version"],
        canonical_url=row["canonical_url"],
        title=row["title"],
        abstract=row["abstract"],
        authors=json.loads(row["authors_json"]),
        primary_category=row["primary_category"],
        categories=json.loads(row["categories_json"]),
        published_at=from_isoformat_or_none(row["published_at"]),
        updated_at=from_isoformat_or_none(row["updated_at"]),
        comment=row["comment"],
        journal_ref=row["journal_ref"],
        doi=row["doi"],
        raw_atom=json.loads(row["raw_atom_json"]),
    )
