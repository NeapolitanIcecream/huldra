from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from huldra.keys import arxiv_id_family_base, arxiv_version, normalize_arxiv_id, request_cache_key
from huldra.migrations import apply_migrations
from huldra.models import (
    ArxivPaper,
    ArxivRequest,
    BrokerStatus,
    CacheEntry,
    CoverageStatus,
    LegacySyncMode,
    OaiHarvestRequest,
    OaiHarvestResult,
    OaiRecord,
    QueueItem,
    QueueWorkKind,
    RateState,
    RequestStatus,
    RetentionGCResult,
    StoreVacuumResult,
)
from huldra.time import ensure_utc, from_isoformat_or_none, isoformat_or_none, utc_now

_TERMINAL_SYNC_JOB_STATUSES = (
    "completed",
    "failed",
    "rate_limited",
    "cache_unreadable",
    "skipped",
    "partial",
    "overflow",
)
_TERMINAL_SYNC_JOB_PLACEHOLDERS = ",".join("?" for _ in _TERMINAL_SYNC_JOB_STATUSES)
_ASYNC_SYNC_JOB_HANDOFF_STATUSES = ("queued", "delayed", "claimed")
_ASYNC_SYNC_JOB_HANDOFF_PLACEHOLDERS = ",".join(
    "?" for _ in _ASYNC_SYNC_JOB_HANDOFF_STATUSES
)
_RETENTION_TABLES = frozenset(
    {
        "cache_entries",
        "events",
        "queue_items",
        "queue_item_upstream_budgets",
        "sync_jobs",
        "sync_job_pages",
        "upstream_request_budgets",
    }
)
# Async maintenance sets completed_at when handing work to a worker. Reclaim
# that job only after all work records for every page are old and terminal.
_RETENTION_SYNC_JOB_FILTER = f"""
    job.completed_at IS NOT NULL
    AND job.completed_at < ?
    AND (
        job.status IN ({_TERMINAL_SYNC_JOB_PLACEHOLDERS})
        OR (
            job.status IN ({_ASYNC_SYNC_JOB_HANDOFF_PLACEHOLDERS})
            AND EXISTS (
                SELECT 1
                FROM sync_job_pages AS page
                WHERE page.sync_job_id = job.sync_job_id
            )
            AND NOT EXISTS (
                SELECT 1
                FROM sync_job_pages AS page
                WHERE page.sync_job_id = job.sync_job_id
                  AND (
                      EXISTS (
                          SELECT 1
                          FROM cache_entries AS non_expired_cache
                          WHERE non_expired_cache.cache_key = page.cache_key
                            AND (
                                non_expired_cache.status NOT IN ('completed', 'failed')
                                OR COALESCE(
                                    CASE non_expired_cache.status
                                        WHEN 'completed' THEN non_expired_cache.completed_at
                                        WHEN 'failed' THEN non_expired_cache.requested_at
                                    END >= ?,
                                    1
                                )
                            )
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM queue_items AS non_expired_queue
                          WHERE non_expired_queue.cache_key = page.cache_key
                            AND (
                                non_expired_queue.status NOT IN ('completed', 'failed')
                                OR COALESCE(
                                    CASE non_expired_queue.status
                                        WHEN 'completed' THEN non_expired_queue.completed_at
                                        WHEN 'failed' THEN non_expired_queue.updated_at
                                    END >= ?,
                                    1
                                )
                            )
                      )
                      OR (
                          NOT EXISTS (
                              SELECT 1
                              FROM cache_entries AS terminal_cache
                              WHERE terminal_cache.cache_key = page.cache_key
                                AND terminal_cache.status IN ('completed', 'failed')
                                AND CASE terminal_cache.status
                                    WHEN 'completed' THEN terminal_cache.completed_at
                                    WHEN 'failed' THEN terminal_cache.requested_at
                                END < ?
                          )
                          AND NOT EXISTS (
                              SELECT 1
                              FROM queue_items AS terminal_queue
                              WHERE terminal_queue.cache_key = page.cache_key
                                AND terminal_queue.status IN ('completed', 'failed')
                                AND CASE terminal_queue.status
                                    WHEN 'completed' THEN terminal_queue.completed_at
                                    WHEN 'failed' THEN terminal_queue.updated_at
                                END < ?
                          )
                      )
                  )
            )
        )
    )
"""


def _retention_sync_job_params(cutoff: str) -> tuple[str, ...]:
    return (
        cutoff,
        *_TERMINAL_SYNC_JOB_STATUSES,
        *_ASYNC_SYNC_JOB_HANDOFF_STATUSES,
        cutoff,
        cutoff,
        cutoff,
        cutoff,
    )


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
    def connect_readonly(self) -> Iterator[sqlite3.Connection]:
        database_uri = f"{self.db_path.resolve().as_uri()}?mode=ro"
        conn = sqlite3.connect(database_uri, timeout=self.timeout, uri=True)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA query_only=ON")
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
                self._upsert_paper_conn(conn, paper, timestamp)

    def _upsert_paper_conn(
        self,
        conn: sqlite3.Connection,
        paper: ArxivPaper,
        timestamp: str,
    ) -> str:
        paper = _legacy_paper_for_existing_oai_base_row(conn, paper)
        conn.execute(
            """
            INSERT INTO papers (
                arxiv_id, version, canonical_url, title, abstract,
                authors_json, primary_category, categories_json,
                published_at, updated_at, comment, journal_ref, doi,
                raw_atom_json, authors_detail_json, license,
                oai_identifier, oai_datestamp, oai_set_specs_json,
                links_json, versions_json, withdrawn, deleted,
                raw_metadata_json, first_seen_at, last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                authors_detail_json=excluded.authors_detail_json,
                license=excluded.license,
                oai_identifier=CASE
                    WHEN excluded.oai_identifier IS NULL THEN oai_identifier
                    ELSE excluded.oai_identifier
                END,
                oai_datestamp=CASE
                    WHEN excluded.oai_identifier IS NULL THEN oai_datestamp
                    ELSE excluded.oai_datestamp
                END,
                oai_set_specs_json=CASE
                    WHEN excluded.oai_identifier IS NULL THEN oai_set_specs_json
                    ELSE excluded.oai_set_specs_json
                END,
                links_json=excluded.links_json,
                versions_json=excluded.versions_json,
                withdrawn=excluded.withdrawn,
                deleted=CASE
                    WHEN excluded.oai_identifier IS NULL THEN deleted
                    ELSE excluded.deleted
                END,
                raw_metadata_json=excluded.raw_metadata_json,
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
                json.dumps(paper.authors_detail, sort_keys=True, separators=(",", ":")),
                paper.license,
                paper.oai_identifier,
                isoformat_or_none(paper.oai_datestamp),
                json.dumps(paper.oai_set_specs, sort_keys=True, separators=(",", ":")),
                json.dumps(paper.links, sort_keys=True, separators=(",", ":")),
                json.dumps(paper.versions, sort_keys=True, separators=(",", ":")),
                int(paper.withdrawn),
                int(paper.deleted),
                json.dumps(paper.raw_metadata, sort_keys=True, separators=(",", ":")),
                timestamp,
                timestamp,
            ),
        )
        return paper.arxiv_id

    def record_completed_cache_entry(
        self,
        *,
        cache_key: str,
        request: ArxivRequest,
        papers: list[ArxivPaper],
        total_results: int | None = None,
        coverage_status: CoverageStatus = CoverageStatus.SLICE,
        upstream_status: int = 200,
        upstream_request_count: int = 1,
        requested_at: datetime | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        requested_value = ensure_utc(requested_at or utc_now())
        completed_value = ensure_utc(completed_at or utc_now())
        requested = isoformat_or_none(requested_value)
        completed = isoformat_or_none(completed_value)
        refresh_after = isoformat_or_none(
            completed_value + timedelta(seconds=request.refresh_interval_seconds)
        )
        assert requested is not None and completed is not None and refresh_after is not None
        with self.begin_immediate() as conn:
            matched_arxiv_ids = list(
                dict.fromkeys(self._upsert_paper_conn(conn, paper, completed) for paper in papers)
            )
            previous = conn.execute(
                "SELECT upstream_requests_total FROM cache_entries WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
            upstream_total = (
                int(previous["upstream_requests_total"]) + upstream_request_count
                if previous
                else upstream_request_count
            )
            conn.execute(
                """
                INSERT INTO cache_entries (
                    cache_key, request_json, api_family, status, requested_at,
                    completed_at, refresh_after, cooldown_until, upstream_status,
                    upstream_requests_total, result_count, total_results,
                    coverage_status, error_category, error_message
                )
                VALUES (?, ?, ?, 'completed', ?, ?, ?, NULL, ?, ?, ?, ?, ?, NULL, NULL)
                ON CONFLICT(cache_key) DO UPDATE SET
                    request_json=excluded.request_json,
                    api_family=excluded.api_family,
                    status='completed',
                    requested_at=excluded.requested_at,
                    completed_at=excluded.completed_at,
                    refresh_after=excluded.refresh_after,
                    cooldown_until=NULL,
                    upstream_status=excluded.upstream_status,
                    upstream_requests_total=excluded.upstream_requests_total,
                    result_count=excluded.result_count,
                    total_results=excluded.total_results,
                    coverage_status=excluded.coverage_status,
                    error_category=NULL,
                    error_message=NULL
                """,
                (
                    cache_key,
                    _request_json(request),
                    request.api_family,
                    requested,
                    completed,
                    refresh_after,
                    upstream_status,
                    upstream_total,
                    len(matched_arxiv_ids),
                    total_results,
                    coverage_status,
                ),
            )
            conn.execute("DELETE FROM cache_matches WHERE cache_key = ?", (cache_key,))
            for position, arxiv_id in enumerate(matched_arxiv_ids):
                conn.execute(
                    """
                    INSERT INTO cache_matches(cache_key, arxiv_id, sort_position, matched_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (cache_key, arxiv_id, position, completed),
                )
            self._record_event_conn(
                conn,
                "fetch_success",
                {
                    "cache_key": cache_key,
                    "papers_total": len(matched_arxiv_ids),
                    "upstream_status": upstream_status,
                    "coverage_status": coverage_status,
                },
            )

    def get_cache_entry(self, cache_key: str) -> CacheEntry | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM cache_entries WHERE cache_key = ?", (cache_key,)).fetchone()
        return _cache_entry_from_row(row) if row else None

    def get_readable_completed_cache(self, cache_key: str) -> CacheEntry | None:
        entry = self.get_cache_entry(cache_key)
        if entry is None or entry.status != "completed":
            return None
        reason = self._completed_cache_integrity_failure(cache_key, entry)
        if reason is None:
            return entry
        self.record_event(
            "cache_integrity_failure",
            {
                "cache_key": cache_key,
                "reason": reason,
                "result_count": entry.result_count,
            },
        )
        return None

    def _completed_cache_integrity_failure(self, cache_key: str, entry: CacheEntry) -> str | None:
        with self.connect() as conn:
            match_count = conn.execute(
                "SELECT COUNT(*) AS total FROM cache_matches WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
            joined_count = conn.execute(
                """
                SELECT COUNT(*) AS total FROM cache_matches m
                JOIN papers p ON p.arxiv_id = m.arxiv_id
                WHERE m.cache_key = ?
                """,
                (cache_key,),
            ).fetchone()
            positions = [
                int(row["sort_position"])
                for row in conn.execute(
                    "SELECT sort_position FROM cache_matches WHERE cache_key = ? ORDER BY sort_position ASC",
                    (cache_key,),
                ).fetchall()
            ]
        if int(match_count["total"]) != entry.result_count:
            return "match_count_mismatch"
        if int(joined_count["total"]) != entry.result_count:
            return "paper_join_mismatch"
        if positions != list(range(entry.result_count)):
            return "sort_position_gap"
        return None

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

    def get_papers_by_ids(self, arxiv_ids: list[str] | tuple[str, ...]) -> dict[str, ArxivPaper]:
        if not arxiv_ids:
            return {}
        requested_ids = tuple(dict.fromkeys(arxiv_ids))
        placeholders = ",".join("?" for _ in requested_ids)
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM papers WHERE arxiv_id IN ({placeholders})",
                requested_ids,
            ).fetchall()
            rows_by_id = {row["arxiv_id"]: row for row in rows}
            papers_by_requested_id: dict[str, ArxivPaper] = {}
            for arxiv_id in requested_ids:
                row = rows_by_id.get(arxiv_id)
                if row is None:
                    row = _oai_base_paper_row_for_versioned_read(conn, arxiv_id)
                if row is None:
                    row = _latest_versioned_paper_row_for_base_read(conn, arxiv_id)
                if row is not None:
                    papers_by_requested_id[arxiv_id] = _paper_from_row(row)
        return papers_by_requested_id

    def get_paper(self, arxiv_id: str) -> ArxivPaper | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM papers WHERE arxiv_id = ?", (arxiv_id,)).fetchone()
            if row is None:
                row = _oai_base_paper_row_for_versioned_read(conn, arxiv_id)
            if row is None:
                row = _latest_versioned_paper_row_for_base_read(conn, arxiv_id)
        return _paper_from_row(row) if row else None

    def mark_paper_deleted(
        self,
        arxiv_id: str,
        *,
        oai_identifier: str | None = None,
        oai_datestamp: datetime | None = None,
        oai_set_specs: list[str] | None = None,
        now: datetime | None = None,
    ) -> None:
        timestamp = isoformat_or_none(now or utc_now())
        assert timestamp is not None
        with self.begin_immediate() as conn:
            conn.execute(
                """
                UPDATE papers
                SET deleted=1,
                    oai_identifier=COALESCE(?, oai_identifier),
                    oai_datestamp=COALESCE(?, oai_datestamp),
                    oai_set_specs_json=?,
                    last_seen_at=?
                WHERE arxiv_id=?
                """,
                (
                    oai_identifier,
                    isoformat_or_none(oai_datestamp),
                    json.dumps(oai_set_specs or [], sort_keys=True, separators=(",", ":")),
                    timestamp,
                    arxiv_id,
                ),
            )

    def create_upstream_request_budget(
        self,
        *,
        max_requests: int,
        deadline_at: datetime | None,
    ) -> str:
        if max_requests <= 0:
            raise ValueError("max_requests must be positive")
        budget_id = str(uuid4())
        timestamp = isoformat_or_none(utc_now())
        deadline = isoformat_or_none(ensure_utc(deadline_at)) if deadline_at is not None else None
        assert timestamp is not None
        with self.begin_immediate() as conn:
            conn.execute(
                """
                INSERT INTO upstream_request_budgets(
                    budget_id, max_requests, requests_started,
                    deadline_at, created_at, updated_at
                )
                VALUES (?, ?, 0, ?, ?, ?)
                """,
                (budget_id, max_requests, deadline, timestamp, timestamp),
            )
            self._record_event_conn(
                conn,
                "upstream_budget_created",
                {
                    "budget_id": budget_id,
                    "max_requests": max_requests,
                    "deadline_at": deadline,
                },
            )
        return budget_id

    def reserve_upstream_request(
        self,
        budget_id: str,
        *,
        now: datetime | None = None,
    ) -> str | None:
        """Atomically consume one upstream attempt or return a terminal budget error."""
        with self.begin_immediate() as conn:
            current = ensure_utc(now or utc_now())
            return self._reserve_upstream_budgets_conn(
                conn,
                (budget_id,),
                current=current,
            )

    def reserve_queue_item_upstream_request(
        self,
        request_id: str,
        *,
        now: datetime | None = None,
    ) -> str | None:
        """Charge every viable joined budget without cancelling other demand."""
        with self.begin_immediate() as conn:
            current = ensure_utc(now or utc_now())
            state = _queue_item_upstream_budget_attempt_state_conn(conn, request_id)
            all_budget_ids = (*state.active_budget_ids, *state.future_budget_ids)
            invalid_budgets = _upstream_budget_errors_conn(
                conn,
                all_budget_ids,
                current=current,
                enforce_request_limit=False,
            )
            capacity_errors = _upstream_budget_errors_conn(
                conn,
                (*state.uncharged_budget_ids, *state.future_budget_ids),
                current=current,
                enforce_request_limit=True,
            )
            for budget_id, error_category in capacity_errors.items():
                invalid_budgets.setdefault(budget_id, error_category)
            if invalid_budgets:
                self._detach_invalid_queue_budgets_conn(
                    conn,
                    request_id=request_id,
                    invalid_budgets=invalid_budgets,
                )
            valid_active_budget_ids = tuple(
                budget_id
                for budget_id in state.active_budget_ids
                if budget_id not in invalid_budgets
            )
            valid_future_budget_ids = tuple(
                budget_id
                for budget_id in state.future_budget_ids
                if budget_id not in invalid_budgets
            )
            valid_uncharged_budget_ids = tuple(
                budget_id
                for budget_id in state.uncharged_budget_ids
                if budget_id not in invalid_budgets
            )
            if not state.unbudgeted_demand and not valid_active_budget_ids:
                if valid_future_budget_ids:
                    return "budget_deferred"
                return _preferred_upstream_budget_error(invalid_budgets)
            budget_error = self._reserve_upstream_budgets_conn(
                conn,
                valid_uncharged_budget_ids,
                current=current,
            )
            if budget_error is not None:
                return budget_error
            if valid_uncharged_budget_ids:
                placeholders = ",".join("?" for _ in valid_uncharged_budget_ids)
                conn.execute(
                    f"""
                    UPDATE queue_item_upstream_budgets
                    SET last_charged_attempt=?
                    WHERE request_id=?
                      AND budget_id IN ({placeholders})
                    """,
                    (
                        state.attempt_number,
                        request_id,
                        *valid_uncharged_budget_ids,
                    ),
                )
            conn.execute(
                """
                UPDATE queue_items
                SET upstream_budget_gate_closed=1
                WHERE request_id=? AND status='claimed'
                """,
                (request_id,),
            )
            return None

    def _detach_invalid_queue_budgets_conn(
        self,
        conn: sqlite3.Connection,
        *,
        request_id: str,
        invalid_budgets: dict[str, str],
    ) -> None:
        for budget_id, error_category in invalid_budgets.items():
            self._record_event_conn(
                conn,
                "upstream_budget_exhausted",
                {
                    "budget_id": budget_id,
                    "request_id": request_id,
                    "error_category": error_category,
                },
            )
        _detach_queue_budget_memberships_conn(
            conn,
            request_id,
            tuple(invalid_budgets),
        )

    def _reserve_upstream_budgets_conn(
        self,
        conn: sqlite3.Connection,
        budget_ids: tuple[str, ...],
        *,
        current: datetime,
    ) -> str | None:
        if not budget_ids:
            return None
        current_s = isoformat_or_none(current)
        assert current_s is not None
        budgets: list[sqlite3.Row] = []
        for budget_id in budget_ids:
            budget = conn.execute(
                "SELECT * FROM upstream_request_budgets WHERE budget_id=?",
                (budget_id,),
            ).fetchone()
            error_category: str | None = None
            if budget is None:
                error_category = "request_budget_exceeded"
            else:
                deadline_at = from_isoformat_or_none(budget["deadline_at"])
                if deadline_at is not None and current >= deadline_at:
                    error_category = "deadline_budget_exceeded"
                elif int(budget["requests_started"]) >= int(budget["max_requests"]):
                    error_category = "request_budget_exceeded"
            if error_category is not None:
                self._record_event_conn(
                    conn,
                    "upstream_budget_exhausted",
                    {
                        "budget_id": budget_id,
                        "error_category": error_category,
                    },
                )
                return error_category
            assert budget is not None
            budgets.append(budget)
        conn.executemany(
            """
            UPDATE upstream_request_budgets
            SET requests_started=requests_started + 1,
                updated_at=?
            WHERE budget_id=?
            """,
            ((current_s, str(budget["budget_id"])) for budget in budgets),
        )
        return None

    def check_upstream_request_deadline(
        self,
        budget_id: str,
        *,
        now: datetime | None = None,
    ) -> str | None:
        """Recheck the durable wall deadline immediately before network I/O."""
        with self.begin_immediate() as conn:
            current = ensure_utc(now or utc_now())
            return _check_upstream_budget_deadlines_conn(
                conn,
                (budget_id,),
                current=current,
            )

    def check_queue_item_upstream_request_deadlines(
        self,
        request_id: str,
        *,
        now: datetime | None = None,
    ) -> str | None:
        """Recheck viable joined budgets after database waits and before I/O."""
        with self.begin_immediate() as conn:
            current = ensure_utc(now or utc_now())
            state = _queue_item_upstream_budget_attempt_state_conn(conn, request_id)
            invalid_budgets = _upstream_budget_errors_conn(
                conn,
                (*state.active_budget_ids, *state.future_budget_ids),
                current=current,
                enforce_request_limit=False,
            )
            if invalid_budgets:
                self._detach_invalid_queue_budgets_conn(
                    conn,
                    request_id=request_id,
                    invalid_budgets=invalid_budgets,
                )
            valid_active_budget_ids = tuple(
                budget_id
                for budget_id in state.active_budget_ids
                if budget_id not in invalid_budgets
            )
            if state.unbudgeted_demand or valid_active_budget_ids:
                return None
            valid_future_budget_ids = tuple(
                budget_id
                for budget_id in state.future_budget_ids
                if budget_id not in invalid_budgets
            )
            if valid_future_budget_ids:
                return "budget_deferred"
            return _preferred_upstream_budget_error(invalid_budgets)

    def queue_item_upstream_budget_ids(self, request_id: str) -> tuple[str, ...]:
        with self.connect() as conn:
            return _queue_item_upstream_budget_ids_conn(conn, request_id)

    def queue_item_upstream_deadline(self, request_id: str) -> datetime | None:
        """Return the earliest deadline funding the queue item's current attempt."""
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT MIN(budget.deadline_at) AS deadline_at
                FROM queue_items AS item
                JOIN queue_item_upstream_budgets AS membership
                  ON membership.request_id=item.request_id
                 AND membership.first_attempt_number <= item.attempts_total
                JOIN upstream_request_budgets AS budget
                  ON budget.budget_id=membership.budget_id
                WHERE item.request_id=?
                  AND budget.deadline_at IS NOT NULL
                """,
                (request_id,),
            ).fetchone()
        return from_isoformat_or_none(row["deadline_at"]) if row is not None else None

    def fail_active_upstream_budget_items(
        self,
        budget_id: str,
        *,
        error_category: str,
        error_message: str,
        now: datetime | None = None,
    ) -> int:
        timestamp = isoformat_or_none(now or utc_now())
        assert timestamp is not None
        with self.begin_immediate() as conn:
            affected = conn.execute(
                """
                SELECT request_id
                FROM queue_items
                WHERE (
                    upstream_budget_id=?
                    OR EXISTS (
                        SELECT 1
                        FROM queue_item_upstream_budgets AS membership
                        WHERE membership.request_id=queue_items.request_id
                          AND membership.budget_id=?
                    )
                )
                  AND status IN ('queued', 'delayed', 'claimed')
                """,
                (budget_id, budget_id),
            ).fetchall()
            failed_total = 0
            for row in affected:
                request_id = str(row["request_id"])
                _detach_queue_budget_memberships_conn(
                    conn,
                    request_id,
                    (budget_id,),
                )
                demand = conn.execute(
                    """
                    SELECT unbudgeted_demand,
                           EXISTS(
                               SELECT 1
                               FROM queue_item_upstream_budgets AS membership
                               WHERE membership.request_id=queue_items.request_id
                           ) AS has_budgeted_demand
                    FROM queue_items
                    WHERE request_id=?
                    """,
                    (request_id,),
                ).fetchone()
                if demand is None or bool(demand["unbudgeted_demand"]):
                    continue
                if bool(demand["has_budgeted_demand"]):
                    continue
                updated = conn.execute(
                    """
                    UPDATE queue_items
                    SET status='failed',
                        updated_at=?,
                        claimed_by=NULL,
                        claimed_until=NULL,
                        upstream_budget_gate_closed=0,
                        next_attempt_at=NULL,
                        error_category=?,
                        error_message=?
                    WHERE request_id=?
                      AND status IN ('queued', 'delayed', 'claimed')
                    """,
                    (
                        timestamp,
                        error_category,
                        error_message[:1000],
                        request_id,
                    ),
                )
                failed_total += updated.rowcount
            if affected:
                self._record_event_conn(
                    conn,
                    "upstream_budget_queue_reconciled",
                    {
                        "budget_id": budget_id,
                        "items_total": len(affected),
                        "detached_total": len(affected),
                        "failed_total": failed_total,
                        "preserved_total": len(affected) - failed_total,
                        "error_category": error_category,
                    },
                )
        return failed_total

    def create_sync_job(
        self,
        request: ArxivRequest,
        mode: LegacySyncMode,
        *,
        upstream_budget_id: str | None = None,
    ) -> str:
        sync_job_id = str(uuid4())
        timestamp = isoformat_or_none(utc_now())
        assert timestamp is not None
        with self.begin_immediate() as conn:
            conn.execute(
                """
                INSERT INTO sync_jobs(
                    sync_job_id, mode, request_json, status, upstream_budget_id,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, 'running', ?, ?, ?)
                """,
                (
                    sync_job_id,
                    mode,
                    _request_json(request),
                    upstream_budget_id,
                    timestamp,
                    timestamp,
                ),
            )
            self._record_event_conn(
                conn,
                "sync_job_started",
                {
                    "sync_job_id": sync_job_id,
                    "mode": mode,
                    "cache_key": request_cache_key(request),
                },
            )
        return sync_job_id

    def record_sync_job_page(
        self,
        *,
        sync_job_id: str,
        request: ArxivRequest,
        cache_key: str,
        status: str,
        result_count: int = 0,
        total_results: int | None = None,
        diagnostics: dict[str, Any] | None = None,
    ) -> None:
        timestamp = isoformat_or_none(utc_now())
        assert timestamp is not None
        with self.begin_immediate() as conn:
            conn.execute(
                """
                INSERT INTO sync_job_pages(
                    sync_job_id, cache_key, request_json, start, max_results,
                    status, result_count, total_results, attempt_diagnostics_json,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(sync_job_id, cache_key) DO UPDATE SET
                    request_json=excluded.request_json,
                    start=excluded.start,
                    max_results=excluded.max_results,
                    status=excluded.status,
                    result_count=excluded.result_count,
                    total_results=excluded.total_results,
                    attempt_diagnostics_json=excluded.attempt_diagnostics_json,
                    updated_at=excluded.updated_at
                """,
                (
                    sync_job_id,
                    cache_key,
                    _request_json(request),
                    request.start,
                    request.max_results,
                    status,
                    result_count,
                    total_results,
                    json.dumps(diagnostics or {}, sort_keys=True, separators=(",", ":")),
                    timestamp,
                    timestamp,
                ),
            )

    def delete_sync_job_followup_pages(
        self,
        *,
        sync_job_id: str,
        initial_cache_key: str,
    ) -> None:
        with self.begin_immediate() as conn:
            conn.execute(
                """
                DELETE FROM sync_job_pages
                WHERE sync_job_id=? AND cache_key<>?
                """,
                (sync_job_id, initial_cache_key),
            )

    def refresh_sync_job_page_from_cache(
        self,
        *,
        sync_job_id: str,
        request: ArxivRequest,
        cache_key: str,
    ) -> dict[str, Any]:
        entry = self.get_cache_entry(cache_key)
        diagnostics: dict[str, Any] = {}
        if entry is None:
            status = "missing"
            result_count = 0
            total_results = None
        else:
            status = entry.status
            result_count = entry.result_count
            total_results = entry.total_results
            diagnostics = {
                "upstream_status": entry.upstream_status,
                "error_category": entry.error_category,
            }
        self.record_sync_job_page(
            sync_job_id=sync_job_id,
            request=request,
            cache_key=cache_key,
            status=status,
            result_count=result_count,
            total_results=total_results,
            diagnostics=diagnostics,
        )
        return {
            "status": status,
            "result_count": result_count,
            "total_results": total_results,
            "error_category": diagnostics.get("error_category"),
        }

    def complete_sync_job(
        self,
        *,
        sync_job_id: str,
        status: str,
        coverage_status: CoverageStatus,
        result_count: int,
        total_results: int | None,
        pages_total: int,
        pages_completed_total: int,
        error_category: str | None = None,
        error_message: str | None = None,
    ) -> None:
        timestamp = isoformat_or_none(utc_now())
        assert timestamp is not None
        with self.begin_immediate() as conn:
            conn.execute(
                """
                UPDATE sync_jobs
                SET status=?,
                    coverage_status=?,
                    result_count=?,
                    total_results=?,
                    pages_total=?,
                    pages_completed_total=?,
                    error_category=?,
                    error_message=?,
                    updated_at=?,
                    completed_at=?
                WHERE sync_job_id=?
                """,
                (
                    status,
                    coverage_status,
                    result_count,
                    total_results,
                    pages_total,
                    pages_completed_total,
                    error_category,
                    error_message[:1000] if error_message else None,
                    timestamp,
                    timestamp,
                    sync_job_id,
                ),
            )
            self._record_event_conn(
                conn,
                "sync_job_completed",
                {
                    "sync_job_id": sync_job_id,
                    "status": status,
                    "coverage_status": coverage_status,
                    "result_count": result_count,
                    "total_results": total_results,
                    "pages_total": pages_total,
                    "pages_completed_total": pages_completed_total,
                    "error_category": error_category,
                },
            )

    def get_sync_job(self, sync_job_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM sync_jobs WHERE sync_job_id = ?",
                (sync_job_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "sync_job_id": row["sync_job_id"],
            "mode": row["mode"],
            "status": row["status"],
            "coverage_status": row["coverage_status"],
            "result_count": int(row["result_count"]),
            "total_results": row["total_results"],
            "pages_total": int(row["pages_total"]),
            "pages_completed_total": int(row["pages_completed_total"]),
            "error_category": row["error_category"],
            "error_message": row["error_message"],
        }

    def create_oai_harvest_job(
        self,
        request: OaiHarvestRequest,
        *,
        resumption_token: str | None = None,
    ) -> str:
        harvest_id = str(uuid4())
        started = utc_now()
        timestamp = isoformat_or_none(started)
        deadline = isoformat_or_none(
            started + timedelta(seconds=request.runtime_budget_seconds)
        )
        assert timestamp is not None and deadline is not None
        with self.begin_immediate() as conn:
            conn.execute(
                """
                INSERT INTO oai_harvest_jobs(
                    harvest_id, request_json, status, metadata_prefix, set_spec,
                    mode, resumption_token, deadline_at, started_at, updated_at
                )
                VALUES (?, ?, 'running', ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    harvest_id,
                    _oai_request_json(request),
                    request.metadata_prefix,
                    request.set_spec,
                    request.mode,
                    resumption_token,
                    deadline,
                    timestamp,
                    timestamp,
                ),
            )
            self._record_event_conn(
                conn,
                "oai_harvest_started",
                {
                    "harvest_id": harvest_id,
                    "metadata_prefix": request.metadata_prefix,
                    "set_spec": request.set_spec,
                    "mode": request.mode,
                },
            )
        return harvest_id

    def get_running_oai_harvest(self, request: OaiHarvestRequest) -> dict[str, Any] | None:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM oai_harvest_jobs
                WHERE status = 'running'
                  AND metadata_prefix = ?
                  AND COALESCE(set_spec, '') = ?
                ORDER BY COALESCE(updated_at, started_at) DESC
                LIMIT 50
                """,
                (request.metadata_prefix, _oai_set_key(request.set_spec)),
            ).fetchall()
        for row in rows:
            try:
                previous = _oai_request_from_json(row["request_json"])
            except ValueError:
                continue
            if not _same_oai_resume_scope(previous, request):
                continue
            if (
                request.resumption_token is not None
                and row["resumption_token"] != request.resumption_token
            ):
                continue
            return _oai_harvest_state_from_row(row)
        return None

    def record_oai_request_started(self, harvest_id: str) -> int:
        timestamp = isoformat_or_none(utc_now())
        assert timestamp is not None
        with self.begin_immediate() as conn:
            conn.execute(
                """
                UPDATE oai_harvest_jobs
                SET requests_total=requests_total + 1,
                    updated_at=?
                WHERE harvest_id=? AND status='running'
                """,
                (timestamp, harvest_id),
            )
            row = conn.execute(
                "SELECT requests_total FROM oai_harvest_jobs WHERE harvest_id=?",
                (harvest_id,),
            ).fetchone()
        if row is None:
            raise ValueError(f"unknown OAI harvest: {harvest_id}")
        return int(row["requests_total"])

    def ensure_oai_harvest_deadline(
        self,
        harvest_id: str,
        deadline_at: datetime,
    ) -> datetime:
        deadline = isoformat_or_none(deadline_at)
        timestamp = isoformat_or_none(utc_now())
        assert deadline is not None and timestamp is not None
        with self.begin_immediate() as conn:
            conn.execute(
                """
                UPDATE oai_harvest_jobs
                SET deadline_at=COALESCE(deadline_at, ?),
                    updated_at=?
                WHERE harvest_id=? AND status='running'
                """,
                (deadline, timestamp, harvest_id),
            )
            row = conn.execute(
                "SELECT deadline_at FROM oai_harvest_jobs WHERE harvest_id=?",
                (harvest_id,),
            ).fetchone()
        if row is None:
            raise ValueError(f"unknown OAI harvest: {harvest_id}")
        persisted = from_isoformat_or_none(row["deadline_at"])
        if persisted is None:
            raise ValueError(f"OAI harvest is not running: {harvest_id}")
        return persisted

    def record_oai_page(
        self,
        *,
        harvest_id: str,
        page_index: int,
        request_params: dict[str, str],
        status: str,
        response_date: str | None = None,
        records_count: int = 0,
        resumption_token_hash: str | None = None,
        next_resumption_token_hash: str | None = None,
        error_category: str | None = None,
        error_message: str | None = None,
    ) -> None:
        timestamp = isoformat_or_none(utc_now())
        assert timestamp is not None
        with self.begin_immediate() as conn:
            conn.execute(
                """
                INSERT INTO oai_pages(
                    harvest_id, page_index, request_params_json,
                    resumption_token_hash, status, response_date, records_count,
                    next_resumption_token_hash, error_category, error_message, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(harvest_id, page_index) DO UPDATE SET
                    request_params_json=excluded.request_params_json,
                    resumption_token_hash=excluded.resumption_token_hash,
                    status=excluded.status,
                    response_date=excluded.response_date,
                    records_count=excluded.records_count,
                    next_resumption_token_hash=excluded.next_resumption_token_hash,
                    error_category=excluded.error_category,
                    error_message=excluded.error_message
                """,
                (
                    harvest_id,
                    page_index,
                    json.dumps(request_params, sort_keys=True, separators=(",", ":")),
                    resumption_token_hash,
                    status,
                    response_date,
                    records_count,
                    next_resumption_token_hash,
                    error_category,
                    error_message[:1000] if error_message else None,
                    timestamp,
                ),
            )

    def checkpoint_oai_page(
        self,
        *,
        harvest_id: str,
        page_index: int,
        request_params: dict[str, str],
        records: list[OaiRecord],
        response_date: str | None,
        consumed_resumption_token: str | None,
        consumed_resumption_token_hash: str | None,
        next_resumption_token: str | None,
        next_resumption_token_hash: str | None,
        last_response_date: str | None,
        last_datestamp_seen: str | None,
        terminal_error_category: str | None = None,
        terminal_error_message: str | None = None,
    ) -> tuple[int, int, int]:
        """Commit records, page audit data, and the next cursor as one checkpoint."""
        timestamp = isoformat_or_none(utc_now())
        assert timestamp is not None
        with self.begin_immediate() as conn:
            job = conn.execute(
                """
                SELECT status, pages_total, resumption_token
                FROM oai_harvest_jobs
                WHERE harvest_id=?
                """,
                (harvest_id,),
            ).fetchone()
            if (
                job is None
                or job["status"] != "running"
                or int(job["pages_total"]) != page_index
                or job["resumption_token"] != consumed_resumption_token
            ):
                raise RuntimeError("OAI page checkpoint no longer matches the durable cursor")
            processed, upserted, deleted = self._upsert_oai_records_conn(
                conn,
                records,
                timestamp,
            )
            conn.execute(
                """
                INSERT INTO oai_pages(
                    harvest_id, page_index, request_params_json,
                    resumption_token_hash, status, response_date, records_count,
                    next_resumption_token_hash, error_category, error_message, created_at
                )
                VALUES (?, ?, ?, ?, 'completed', ?, ?, ?, NULL, NULL, ?)
                ON CONFLICT(harvest_id, page_index) DO UPDATE SET
                    request_params_json=excluded.request_params_json,
                    resumption_token_hash=excluded.resumption_token_hash,
                    status='completed',
                    response_date=excluded.response_date,
                    records_count=excluded.records_count,
                    next_resumption_token_hash=excluded.next_resumption_token_hash,
                    error_category=NULL,
                    error_message=NULL
                """,
                (
                    harvest_id,
                    page_index,
                    json.dumps(request_params, sort_keys=True, separators=(",", ":")),
                    consumed_resumption_token_hash,
                    response_date,
                    len(records),
                    next_resumption_token_hash,
                    timestamp,
                ),
            )
            updated = conn.execute(
                """
                UPDATE oai_harvest_jobs
                SET records_processed=records_processed + ?,
                    papers_upserted=papers_upserted + ?,
                    deleted_records=deleted_records + ?,
                    pages_total=MAX(pages_total, ?),
                    resumption_token=?,
                    last_response_date=COALESCE(?, last_response_date),
                    last_datestamp_seen=COALESCE(?, last_datestamp_seen),
                    finished_paging=?,
                    error_category=?,
                    error_message=?,
                    updated_at=?
                WHERE harvest_id=? AND status='running'
                """,
                (
                    processed,
                    upserted,
                    deleted,
                    page_index + 1,
                    next_resumption_token,
                    last_response_date,
                    last_datestamp_seen,
                    int(next_resumption_token is None),
                    terminal_error_category,
                    (
                        terminal_error_message[:1000]
                        if terminal_error_message is not None
                        else None
                    ),
                    timestamp,
                    harvest_id,
                ),
            )
            if updated.rowcount != 1:
                raise ValueError(f"OAI harvest is not running: {harvest_id}")
        return processed, upserted, deleted

    def get_oai_consumed_token_hashes(self, harvest_id: str) -> set[str]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT resumption_token_hash
                FROM oai_pages
                WHERE harvest_id=? AND resumption_token_hash IS NOT NULL
                """,
                (harvest_id,),
            ).fetchall()
        return {str(row["resumption_token_hash"]) for row in rows}

    def upsert_oai_records(
        self,
        records: list[OaiRecord],
        *,
        now: datetime | None = None,
    ) -> tuple[int, int, int]:
        if not records:
            return (0, 0, 0)
        timestamp = isoformat_or_none(now or utc_now())
        assert timestamp is not None
        with self.begin_immediate() as conn:
            return self._upsert_oai_records_conn(conn, records, timestamp)

    def _upsert_oai_records_conn(
        self,
        conn: sqlite3.Connection,
        records: list[OaiRecord],
        timestamp: str,
    ) -> tuple[int, int, int]:
        papers_upserted = 0
        for record in records:
            if record.paper is not None and not record.deleted:
                self._upsert_paper_conn(
                    conn,
                    _oai_paper_for_existing_version_family(conn, record.paper),
                    timestamp,
                )
                papers_upserted += 1
            existing = conn.execute(
                """
                SELECT first_seen_at FROM oai_records
                WHERE oai_identifier = ? AND metadata_prefix = ?
                """,
                (record.oai_identifier, record.metadata_prefix),
            ).fetchone()
            first_seen = existing["first_seen_at"] if existing else timestamp
            conn.execute(
                """
                INSERT INTO oai_records(
                    oai_identifier, metadata_prefix, arxiv_id, datestamp,
                    deleted, set_specs_json, raw_xml, first_seen_at, last_seen_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(oai_identifier, metadata_prefix) DO UPDATE SET
                    arxiv_id=excluded.arxiv_id,
                    datestamp=excluded.datestamp,
                    deleted=excluded.deleted,
                    set_specs_json=excluded.set_specs_json,
                    raw_xml=excluded.raw_xml,
                    last_seen_at=excluded.last_seen_at
                """,
                (
                    record.oai_identifier,
                    record.metadata_prefix,
                    record.arxiv_id,
                    isoformat_or_none(record.datestamp),
                    int(record.deleted),
                    json.dumps(record.set_specs, sort_keys=True, separators=(",", ":")),
                    record.raw_xml,
                    first_seen,
                    timestamp,
                ),
            )
            if record.deleted and record.arxiv_id:
                arxiv_ids = _paper_arxiv_ids_in_version_family(conn, record.arxiv_id)
                if not arxiv_ids:
                    continue
                placeholders = ",".join("?" for _ in arxiv_ids)
                conn.execute(
                    f"""
                    UPDATE papers
                    SET deleted=1,
                        oai_identifier=?,
                        oai_datestamp=COALESCE(?, oai_datestamp),
                        oai_set_specs_json=?,
                        last_seen_at=?
                    WHERE arxiv_id IN ({placeholders})
                    """,
                    (
                        record.oai_identifier,
                        isoformat_or_none(record.datestamp),
                        json.dumps(record.set_specs, sort_keys=True, separators=(",", ":")),
                        timestamp,
                        *arxiv_ids,
                    ),
                )

        return (len(records), papers_upserted, sum(1 for record in records if record.deleted))

    def get_latest_resumable_oai_harvest_token(self, request: OaiHarvestRequest) -> str | None:
        resumable_statuses = (
            "rate_limited",
            "cooling_down",
            "blocked",
            "transient_failure",
            "budget_exceeded",
        )
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT request_json, status, resumption_token
                FROM oai_harvest_jobs
                WHERE metadata_prefix = ?
                  AND COALESCE(set_spec, '') = ?
                ORDER BY completed_at DESC, started_at DESC
                LIMIT 50
                """,
                (
                    request.metadata_prefix,
                    _oai_set_key(request.set_spec),
                ),
            ).fetchall()
        for row in rows:
            try:
                previous = _oai_request_from_json(row["request_json"])
            except ValueError:
                continue
            if not _same_oai_resume_scope(previous, request):
                continue
            if row["status"] not in resumable_statuses:
                return None
            token = row["resumption_token"]
            if isinstance(token, str) and token:
                return token
            return None
        return None

    def get_oai_watermark(
        self,
        *,
        metadata_prefix: str,
        set_spec: str | None,
    ) -> dict[str, str | None] | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM oai_watermarks
                WHERE metadata_prefix = ? AND set_spec = ?
                """,
                (metadata_prefix, _oai_set_key(set_spec)),
            ).fetchone()
        if row is None:
            return None
        return {
            "last_response_date": row["last_response_date"],
            "last_datestamp_seen": row["last_datestamp_seen"],
            "last_successful_harvest_id": row["last_successful_harvest_id"],
        }

    def set_oai_watermark(
        self,
        *,
        metadata_prefix: str,
        set_spec: str | None,
        last_response_date: str | None,
        last_datestamp_seen: str | None,
        harvest_id: str,
    ) -> None:
        timestamp = isoformat_or_none(utc_now())
        assert timestamp is not None
        with self.begin_immediate() as conn:
            conn.execute(
                """
                INSERT INTO oai_watermarks(
                    metadata_prefix, set_spec, last_response_date,
                    last_datestamp_seen, last_successful_harvest_id, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(metadata_prefix, set_spec) DO UPDATE SET
                    last_response_date=CASE
                        WHEN excluded.last_response_date IS NULL
                            THEN oai_watermarks.last_response_date
                        WHEN oai_watermarks.last_response_date IS NULL
                          OR date(excluded.last_response_date)
                             >= date(oai_watermarks.last_response_date)
                            THEN excluded.last_response_date
                        ELSE oai_watermarks.last_response_date
                    END,
                    last_datestamp_seen=CASE
                        WHEN excluded.last_datestamp_seen IS NULL
                            THEN oai_watermarks.last_datestamp_seen
                        WHEN oai_watermarks.last_datestamp_seen IS NULL
                          OR date(excluded.last_datestamp_seen)
                             >= date(oai_watermarks.last_datestamp_seen)
                            THEN excluded.last_datestamp_seen
                        ELSE oai_watermarks.last_datestamp_seen
                    END,
                    last_successful_harvest_id=CASE
                        WHEN (
                            excluded.last_response_date IS NOT NULL
                            AND (
                                oai_watermarks.last_response_date IS NULL
                                OR date(excluded.last_response_date)
                                   >= date(oai_watermarks.last_response_date)
                            )
                        ) OR (
                            excluded.last_datestamp_seen IS NOT NULL
                            AND (
                                oai_watermarks.last_datestamp_seen IS NULL
                                OR date(excluded.last_datestamp_seen)
                                   >= date(oai_watermarks.last_datestamp_seen)
                            )
                        ) THEN excluded.last_successful_harvest_id
                        ELSE oai_watermarks.last_successful_harvest_id
                    END,
                    updated_at=CASE
                        WHEN (
                            excluded.last_response_date IS NOT NULL
                            AND (
                                oai_watermarks.last_response_date IS NULL
                                OR date(excluded.last_response_date)
                                   >= date(oai_watermarks.last_response_date)
                            )
                        ) OR (
                            excluded.last_datestamp_seen IS NOT NULL
                            AND (
                                oai_watermarks.last_datestamp_seen IS NULL
                                OR date(excluded.last_datestamp_seen)
                                   >= date(oai_watermarks.last_datestamp_seen)
                            )
                        ) THEN excluded.updated_at
                        ELSE oai_watermarks.updated_at
                    END
                """,
                (
                    metadata_prefix,
                    _oai_set_key(set_spec),
                    last_response_date,
                    last_datestamp_seen,
                    harvest_id,
                    timestamp,
                ),
            )

    def complete_oai_harvest_job(
        self,
        *,
        harvest_id: str,
        status: str,
        records_processed: int,
        papers_upserted: int,
        deleted_records: int,
        pages_total: int,
        current_watermark: str | None,
        resumption_token: str | None = None,
        error_category: str | None = None,
        error_message: str | None = None,
    ) -> OaiHarvestResult:
        completed = isoformat_or_none(utc_now())
        assert completed is not None
        with self.begin_immediate() as conn:
            conn.execute(
                """
                UPDATE oai_harvest_jobs
                SET status=?,
                    records_processed=?,
                    papers_upserted=?,
                    deleted_records=?,
                    pages_total=?,
                    current_watermark=?,
                    resumption_token=?,
                    error_category=?,
                    error_message=?,
                    updated_at=?,
                    completed_at=?
                WHERE harvest_id=?
                """,
                (
                    status,
                    records_processed,
                    papers_upserted,
                    deleted_records,
                    pages_total,
                    current_watermark,
                    resumption_token,
                    error_category,
                    error_message[:1000] if error_message else None,
                    completed,
                    completed,
                    harvest_id,
                ),
            )
            self._record_event_conn(
                conn,
                "oai_harvest_completed",
                {
                    "harvest_id": harvest_id,
                    "status": status,
                    "records_processed": records_processed,
                    "papers_upserted": papers_upserted,
                    "deleted_records": deleted_records,
                    "pages_total": pages_total,
                    "error_category": error_category,
                },
            )
        result = self.get_oai_harvest_result(harvest_id)
        assert result is not None
        return result

    def get_oai_harvest_result(self, harvest_id: str) -> OaiHarvestResult | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM oai_harvest_jobs WHERE harvest_id = ?",
                (harvest_id,),
            ).fetchone()
        if row is None:
            return None
        return OaiHarvestResult(
            harvest_id=row["harvest_id"],
            status=row["status"],
            metadata_prefix=row["metadata_prefix"],
            set_spec=row["set_spec"],
            mode=row["mode"],
            records_processed=int(row["records_processed"]),
            papers_upserted=int(row["papers_upserted"]),
            deleted_records=int(row["deleted_records"]),
            pages_total=int(row["pages_total"]),
            requests_total=int(row["requests_total"]),
            current_watermark=row["current_watermark"],
            resumption_token=row["resumption_token"],
            error_category=row["error_category"],
            error_message=row["error_message"],
            deadline_at=from_isoformat_or_none(row["deadline_at"]),
        )

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
        upstream_request_count: int = 1,
        requested_at: datetime | None = None,
    ) -> None:
        if upstream_request_count < 0:
            raise ValueError("upstream_request_count cannot be negative")
        timestamp = isoformat_or_none(requested_at or utc_now())
        cooldown = isoformat_or_none(cooldown_until)
        assert timestamp is not None
        with self.begin_immediate() as conn:
            previous = conn.execute(
                "SELECT status, upstream_requests_total FROM cache_entries WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
            final_status = "completed" if previous and previous["status"] == "completed" else status
            upstream_total = (
                int(previous["upstream_requests_total"]) + upstream_request_count
                if previous
                else upstream_request_count
            )
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
        upstream_status: int = 429,
        error_message: str = "arXiv returned HTTP 429",
    ) -> None:
        self.record_cache_failure(
            cache_key=cache_key,
            request=request,
            status="rate_limited",
            cooldown_until=cooldown_until,
            upstream_status=upstream_status,
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
            consecutive_rate_limit_total=int(row["consecutive_rate_limit_total"]),
            upstream_rate_limited_total=int(row["upstream_rate_limited_total"]),
            upstream_oai_503_retry_after_total=int(
                row["upstream_oai_503_retry_after_total"]
            ),
            last_status=row["last_status"],
            last_error_message=row["last_error_message"],
            last_request_started_at=from_isoformat_or_none(row["last_request_started_at"]),
            last_rate_wait_seconds=row["last_rate_wait_seconds"],
            last_request_latency_ms=row["last_request_latency_ms"],
            last_retry_after_seconds=row["last_retry_after_seconds"],
            last_effective_cooldown_seconds=row["last_effective_cooldown_seconds"],
            last_rate_limit_kind=row["last_rate_limit_kind"],
            last_api_family=row["last_api_family"],
        )

    def set_rate_state(self, state: RateState) -> None:
        with self.begin_immediate() as conn:
            conn.execute(
                """
                INSERT INTO rate_state(
                    name, last_request_at, cooldown_until, consecutive_429_total,
                    upstream_429_total, consecutive_rate_limit_total,
                    upstream_rate_limited_total, upstream_oai_503_retry_after_total,
                    last_status, last_error_message, last_request_started_at,
                    last_rate_wait_seconds, last_request_latency_ms,
                    last_retry_after_seconds, last_effective_cooldown_seconds,
                    last_rate_limit_kind, last_api_family
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    last_request_at=excluded.last_request_at,
                    cooldown_until=excluded.cooldown_until,
                    consecutive_429_total=excluded.consecutive_429_total,
                    upstream_429_total=excluded.upstream_429_total,
                    consecutive_rate_limit_total=excluded.consecutive_rate_limit_total,
                    upstream_rate_limited_total=excluded.upstream_rate_limited_total,
                    upstream_oai_503_retry_after_total=excluded.upstream_oai_503_retry_after_total,
                    last_status=excluded.last_status,
                    last_error_message=excluded.last_error_message,
                    last_request_started_at=excluded.last_request_started_at,
                    last_rate_wait_seconds=excluded.last_rate_wait_seconds,
                    last_request_latency_ms=excluded.last_request_latency_ms,
                    last_retry_after_seconds=excluded.last_retry_after_seconds,
                    last_effective_cooldown_seconds=excluded.last_effective_cooldown_seconds,
                    last_rate_limit_kind=excluded.last_rate_limit_kind,
                    last_api_family=excluded.last_api_family
                """,
                (
                    state.name,
                    isoformat_or_none(state.last_request_at),
                    isoformat_or_none(state.cooldown_until),
                    state.consecutive_429_total,
                    state.upstream_429_total,
                    state.consecutive_rate_limit_total,
                    state.upstream_rate_limited_total,
                    state.upstream_oai_503_retry_after_total,
                    state.last_status,
                    state.last_error_message,
                    isoformat_or_none(state.last_request_started_at),
                    state.last_rate_wait_seconds,
                    state.last_request_latency_ms,
                    state.last_retry_after_seconds,
                    state.last_effective_cooldown_seconds,
                    state.last_rate_limit_kind,
                    state.last_api_family,
                ),
            )

    def enqueue_request(self, request: ArxivRequest, cache_key: str | None = None) -> QueueItem:
        item, _joined = self.enqueue_request_for_work(request, cache_key)
        return item

    def enqueue_request_for_work(
        self,
        request: ArxivRequest,
        cache_key: str | None = None,
        *,
        work_kind: QueueWorkKind | None = None,
        upstream_budget_id: str | None = None,
    ) -> tuple[QueueItem, bool]:
        key = cache_key or request_cache_key(request)
        resolved_work_kind = work_kind or (
            QueueWorkKind.REFRESH_COMPLETED
            if request.cache_policy == "stale_while_revalidate"
            else QueueWorkKind.FETCH_MISSING
        )
        now = utc_now()
        now_s = isoformat_or_none(now)
        assert now_s is not None
        with self.begin_immediate() as conn:
            return self._enqueue_request_for_work_conn(
                conn,
                request=request,
                cache_key=key,
                work_kind=resolved_work_kind,
                upstream_budget_id=upstream_budget_id,
                now_s=now_s,
            )

    def enqueue_refresh_if_due(
        self,
        request: ArxivRequest,
        cache_key: str | None = None,
        *,
        now: datetime | None = None,
    ) -> tuple[QueueItem | None, bool]:
        """Atomically reserve a refresh period and enqueue at most one refresh."""
        key = cache_key or request_cache_key(request)
        current = ensure_utc(now or utc_now())
        current_s = isoformat_or_none(current)
        next_refresh = isoformat_or_none(
            current + timedelta(seconds=request.refresh_interval_seconds)
        )
        assert current_s is not None and next_refresh is not None
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
                return self._enqueue_request_for_work_conn(
                    conn,
                    request=request,
                    cache_key=key,
                    work_kind=QueueWorkKind.REFRESH_COMPLETED,
                    upstream_budget_id=None,
                    now_s=current_s,
                )
            cache = conn.execute(
                """
                SELECT status, completed_at, refresh_after
                FROM cache_entries
                WHERE cache_key = ?
                """,
                (key,),
            ).fetchone()
            if cache is None or cache["status"] != "completed":
                return None, False
            refresh_after = from_isoformat_or_none(cache["refresh_after"])
            completed_at = from_isoformat_or_none(cache["completed_at"])
            requested_refresh_after = (
                completed_at + timedelta(seconds=request.refresh_interval_seconds)
                if completed_at is not None
                else None
            )
            effective_refresh_after = refresh_after
            if requested_refresh_after is not None and (
                effective_refresh_after is None
                or requested_refresh_after < effective_refresh_after
            ):
                effective_refresh_after = requested_refresh_after
            if effective_refresh_after is not None and effective_refresh_after > current:
                return None, False
            conn.execute(
                "UPDATE cache_entries SET refresh_after = ? WHERE cache_key = ?",
                (next_refresh, key),
            )
            return self._enqueue_request_for_work_conn(
                conn,
                request=request,
                cache_key=key,
                work_kind=QueueWorkKind.REFRESH_COMPLETED,
                upstream_budget_id=None,
                now_s=current_s,
            )

    def _enqueue_request_for_work_conn(
        self,
        conn: sqlite3.Connection,
        *,
        request: ArxivRequest,
        cache_key: str,
        work_kind: QueueWorkKind,
        upstream_budget_id: str | None,
        now_s: str,
    ) -> tuple[QueueItem, bool]:
        existing = conn.execute(
            """
            SELECT * FROM queue_items
            WHERE cache_key = ? AND status IN ('queued', 'delayed', 'claimed')
            ORDER BY created_at ASC
            LIMIT 1
            """,
            (cache_key,),
        ).fetchone()
        if existing is not None:
            queue_item_changed = False
            replacement_request: ArxivRequest | None = None
            if work_kind == QueueWorkKind.REFRESH_COMPLETED:
                if existing["work_kind"] != QueueWorkKind.REFRESH_COMPLETED:
                    replacement_request = request
                else:
                    existing_request = _request_from_json(existing["request_json"])
                    if (
                        request.refresh_interval_seconds
                        < existing_request.refresh_interval_seconds
                    ):
                        replacement_request = existing_request.model_copy(
                            update={
                                "refresh_interval_seconds": request.refresh_interval_seconds,
                            }
                        )
            if replacement_request is not None:
                conn.execute(
                    """
                    UPDATE queue_items
                    SET work_kind=?, request_json=?, updated_at=?
                    WHERE request_id=?
                    """,
                    (
                        QueueWorkKind.REFRESH_COMPLETED,
                        _request_json(replacement_request),
                        now_s,
                        existing["request_id"],
                    ),
                )
                queue_item_changed = True
            if upstream_budget_id is None and not bool(existing["unbudgeted_demand"]):
                conn.execute(
                    """
                    UPDATE queue_items
                    SET unbudgeted_demand=1, updated_at=?
                    WHERE request_id=?
                    """,
                    (now_s, existing["request_id"]),
                )
                queue_item_changed = True
            if upstream_budget_id is not None:
                attempts_total = int(existing["attempts_total"])
                joins_current_attempt = (
                    existing["status"] == RequestStatus.CLAIMED
                    and not bool(existing["upstream_budget_gate_closed"])
                )
                first_attempt_number = max(
                    1,
                    attempts_total if joins_current_attempt else attempts_total + 1,
                )
                conn.execute(
                    """
                    UPDATE queue_items
                    SET upstream_budget_id=COALESCE(upstream_budget_id, ?),
                        updated_at=?
                    WHERE request_id=?
                    """,
                    (upstream_budget_id, now_s, existing["request_id"]),
                )
                conn.execute(
                    """
                    INSERT OR IGNORE INTO queue_item_upstream_budgets(
                        request_id, budget_id, created_at, first_attempt_number
                    )
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        existing["request_id"],
                        upstream_budget_id,
                        now_s,
                        first_attempt_number,
                    ),
                )
                queue_item_changed = True
            if queue_item_changed:
                existing = conn.execute(
                    "SELECT * FROM queue_items WHERE request_id = ?",
                    (existing["request_id"],),
                ).fetchone()
            assert existing is not None
            return _queue_item_from_row(existing), True
        request_id = str(uuid4())
        conn.execute(
            """
            INSERT INTO queue_items(
                request_id, cache_key, client_id, request_json, priority, status, work_kind,
                upstream_budget_id, unbudgeted_demand, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?)
            """,
            (
                request_id,
                cache_key,
                request.client_id,
                _request_json(request),
                request.priority,
                work_kind,
                upstream_budget_id,
                int(upstream_budget_id is None),
                now_s,
                now_s,
            ),
        )
        if upstream_budget_id is not None:
            conn.execute(
                """
                INSERT INTO queue_item_upstream_budgets(
                    request_id, budget_id, created_at, first_attempt_number
                )
                VALUES (?, ?, ?, 1)
                """,
                (request_id, upstream_budget_id, now_s),
            )
        self._record_event_conn(
            conn,
            "request_enqueued",
            {
                "request_id": request_id,
                "cache_key": cache_key,
                "client_id": request.client_id,
                "work_kind": work_kind,
            },
        )
        row = conn.execute("SELECT * FROM queue_items WHERE request_id = ?", (request_id,)).fetchone()
        assert row is not None
        return _queue_item_from_row(row), False

    def claim_next_queue_item(
        self,
        *,
        owner_token: str,
        claim_timeout_seconds: int = 300,
        cache_keys: set[str] | frozenset[str] | None = None,
        request_ids: set[str] | frozenset[str] | None = None,
        now: datetime | None = None,
    ) -> QueueItem | None:
        if cache_keys is not None and not cache_keys:
            return None
        if request_ids is not None and not request_ids:
            return None
        with self.begin_immediate() as conn:
            current = ensure_utc(now or utc_now())
            current_s = isoformat_or_none(current)
            claim_until = isoformat_or_none(current + timedelta(seconds=claim_timeout_seconds))
            assert current_s is not None and claim_until is not None
            filters: list[str] = []
            params: list[object] = []
            if cache_keys is not None:
                ordered = sorted(cache_keys)
                filters.append(f"cache_key IN ({','.join('?' for _ in ordered)})")
                params.extend(ordered)
            if request_ids is not None:
                ordered_ids = sorted(request_ids)
                filters.append(f"request_id IN ({','.join('?' for _ in ordered_ids)})")
                params.extend(ordered_ids)
            target_filter = f"AND {' AND '.join(filters)}" if filters else ""
            row = conn.execute(
                f"""
                SELECT * FROM queue_items
                WHERE (
                    (
                        status IN ('queued', 'delayed')
                        AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                    ) OR (
                        status = 'claimed'
                        AND claimed_until IS NOT NULL
                        AND claimed_until <= ?
                    )
                )
                {target_filter}
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
                """,
                (current_s, current_s, *params),
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                """
                UPDATE queue_items
                SET status='claimed',
                    claimed_by=?,
                    claimed_until=?,
                    upstream_budget_gate_closed=0,
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

    def renew_queue_claim(
        self,
        request_id: str,
        *,
        owner_token: str,
        claim_timeout_seconds: int,
        now: datetime | None = None,
    ) -> QueueItem | None:
        with self.begin_immediate() as conn:
            current = ensure_utc(now or utc_now())
            current_s = isoformat_or_none(current)
            claim_until = isoformat_or_none(
                current + timedelta(seconds=claim_timeout_seconds)
            )
            assert current_s is not None and claim_until is not None
            updated = conn.execute(
                """
                UPDATE queue_items
                SET claimed_until=?, updated_at=?
                WHERE request_id=?
                  AND status='claimed'
                  AND claimed_by=?
                """,
                (claim_until, current_s, request_id, owner_token),
            )
            if updated.rowcount != 1:
                return None
            row = conn.execute(
                "SELECT * FROM queue_items WHERE request_id=?",
                (request_id,),
            ).fetchone()
        assert row is not None
        return _queue_item_from_row(row)

    def acquire_id_fetch_reservations(
        self,
        arxiv_ids: list[str] | tuple[str, ...],
        *,
        owner_token: str,
        request_id: str,
        ttl_seconds: int,
        now: datetime | None = None,
    ) -> IdReservationResult:
        if not arxiv_ids:
            return IdReservationResult(acquired_ids=(), blocked_until=None)
        ids = tuple(dict.fromkeys(arxiv_ids))
        with self.begin_immediate() as conn:
            current = ensure_utc(now or utc_now())
            current_s = isoformat_or_none(current)
            expires_at = current + timedelta(seconds=ttl_seconds)
            expires_s = isoformat_or_none(expires_at)
            assert current_s is not None and expires_s is not None
            conn.execute(
                "DELETE FROM id_fetch_reservations WHERE expires_at <= ?",
                (current_s,),
            )
            placeholders = ",".join("?" for _ in ids)
            rows = conn.execute(
                f"SELECT * FROM id_fetch_reservations WHERE arxiv_id IN ({placeholders})",
                ids,
            ).fetchall()
            blocked_until_values = [
                from_isoformat_or_none(row["expires_at"])
                for row in rows
                if row["owner_token"] != owner_token
            ]
            blocked_until_values = [value for value in blocked_until_values if value is not None]
            if blocked_until_values:
                return IdReservationResult(
                    acquired_ids=(),
                    blocked_until=min(blocked_until_values),
                )
            for arxiv_id in ids:
                conn.execute(
                    """
                    INSERT INTO id_fetch_reservations(
                        arxiv_id, owner_token, request_id, acquired_at, expires_at
                    )
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(arxiv_id) DO UPDATE SET
                        owner_token=excluded.owner_token,
                        request_id=excluded.request_id,
                        acquired_at=excluded.acquired_at,
                        expires_at=excluded.expires_at
                    """,
                    (arxiv_id, owner_token, request_id, current_s, expires_s),
                )
        return IdReservationResult(acquired_ids=ids, blocked_until=None)

    def renew_id_fetch_reservations(
        self,
        arxiv_ids: list[str] | tuple[str, ...],
        *,
        owner_token: str,
        ttl_seconds: int,
        now: datetime | None = None,
    ) -> bool:
        ids = tuple(dict.fromkeys(arxiv_ids))
        if not ids:
            return True
        with self.begin_immediate() as conn:
            current = ensure_utc(now or utc_now())
            current_s = isoformat_or_none(current)
            expires_s = isoformat_or_none(current + timedelta(seconds=ttl_seconds))
            assert current_s is not None and expires_s is not None
            placeholders = ",".join("?" for _ in ids)
            rows = conn.execute(
                f"SELECT arxiv_id, owner_token FROM id_fetch_reservations "
                f"WHERE arxiv_id IN ({placeholders})",
                ids,
            ).fetchall()
            if len(rows) != len(ids) or any(
                row["owner_token"] != owner_token for row in rows
            ):
                return False
            updated = conn.execute(
                f"""
                UPDATE id_fetch_reservations
                SET acquired_at=?, expires_at=?
                WHERE owner_token=? AND arxiv_id IN ({placeholders})
                """,
                (current_s, expires_s, owner_token, *ids),
            )
        return updated.rowcount == len(ids)

    def release_id_fetch_reservations(
        self,
        arxiv_ids: list[str] | tuple[str, ...],
        *,
        owner_token: str,
    ) -> None:
        if not arxiv_ids:
            return
        ids = tuple(dict.fromkeys(arxiv_ids))
        placeholders = ",".join("?" for _ in ids)
        timestamp = isoformat_or_none(utc_now())
        assert timestamp is not None
        with self.begin_immediate() as conn:
            deleted = conn.execute(
                f"DELETE FROM id_fetch_reservations WHERE owner_token = ? AND arxiv_id IN ({placeholders})",
                (owner_token, *ids),
            )
            if deleted.rowcount:
                self._wake_id_fetch_reserved_queue_items_conn(conn, ids, timestamp)

    def _wake_id_fetch_reserved_queue_items_conn(
        self,
        conn: sqlite3.Connection,
        released_ids: tuple[str, ...],
        timestamp: str,
    ) -> None:
        released = set(released_ids)
        rows = conn.execute(
            """
            SELECT request_id, request_json
            FROM queue_items
            WHERE status = 'delayed' AND error_category = 'id_fetch_reserved'
            """
        ).fetchall()
        request_ids = []
        for row in rows:
            request = _request_from_json(row["request_json"])
            if any(normalize_arxiv_id(value) in released for value in request.id_list):
                request_ids.append(row["request_id"])
        if not request_ids:
            return
        placeholders = ",".join("?" for _ in request_ids)
        conn.execute(
            f"""
            UPDATE queue_items
            SET status='queued',
                next_attempt_at=NULL,
                updated_at=?,
                claimed_by=NULL,
                claimed_until=NULL,
                upstream_budget_gate_closed=0,
                error_category=NULL,
                error_message=NULL
            WHERE request_id IN ({placeholders})
            """,
            (timestamp, *request_ids),
        )

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
                    upstream_budget_gate_closed=0,
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
        with self.begin_immediate() as conn:
            current = ensure_utc(now or utc_now())
            current_s = isoformat_or_none(current)
            expires_s = isoformat_or_none(current + timedelta(seconds=timeout_seconds))
            assert current_s is not None and expires_s is not None
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

    def renew_lease_if_owned(
        self,
        name: str,
        owner_token: str,
        timeout_seconds: int,
        *,
        now: datetime | None = None,
    ) -> bool:
        """Extend an existing lease without ever reacquiring a lost lease."""
        return self.renew_leases_if_owned(
            ((name, owner_token, timeout_seconds),),
            now=now,
        )

    def renew_leases_if_owned(
        self,
        leases: tuple[tuple[str, str, int], ...],
        *,
        now: datetime | None = None,
    ) -> bool:
        """Atomically extend every listed lease only while all owners still match."""
        if not leases:
            return True
        if len({name for name, _owner, _timeout in leases}) != len(leases):
            raise ValueError("lease names must be unique")
        with self.begin_immediate() as conn:
            current = ensure_utc(now or utc_now())
            rows = {
                str(row["name"]): row
                for row in conn.execute(
                    f"SELECT name, owner_token FROM leases WHERE name IN "
                    f"({','.join('?' for _ in leases)})",
                    tuple(name for name, _owner, _timeout in leases),
                ).fetchall()
            }
            if any(
                name not in rows or rows[name]["owner_token"] != owner_token
                for name, owner_token, _timeout in leases
            ):
                return False
            for name, owner_token, timeout_seconds in leases:
                expires_s = isoformat_or_none(
                    current + timedelta(seconds=timeout_seconds)
                )
                assert expires_s is not None
                conn.execute(
                    "UPDATE leases SET expires_at=? WHERE name=? AND owner_token=?",
                    (expires_s, name, owner_token),
                )
        return True

    def renew_worker_fetch_ownership(
        self,
        *,
        request_id: str,
        owner_token: str,
        lease_name: str,
        ttl_seconds: int,
        reserved_ids: tuple[str, ...] = (),
        now: datetime | None = None,
    ) -> str | None:
        """Atomically fence the queue claim, upstream lease, and ID reservations."""
        ids = tuple(dict.fromkeys(reserved_ids))
        placeholders = ",".join("?" for _ in ids)
        with self.begin_immediate() as conn:
            current = ensure_utc(now or utc_now())
            current_s = isoformat_or_none(current)
            expires_s = isoformat_or_none(current + timedelta(seconds=ttl_seconds))
            assert current_s is not None and expires_s is not None
            lease = conn.execute(
                "SELECT owner_token FROM leases WHERE name=?",
                (lease_name,),
            ).fetchone()
            if lease is None or lease["owner_token"] != owner_token:
                return "lost_lease"
            queue_item = conn.execute(
                "SELECT status, claimed_by FROM queue_items WHERE request_id=?",
                (request_id,),
            ).fetchone()
            if (
                queue_item is None
                or queue_item["status"] != "claimed"
                or queue_item["claimed_by"] != owner_token
            ):
                return "lost_claim"
            if ids:
                reservations = conn.execute(
                    f"SELECT arxiv_id, owner_token FROM id_fetch_reservations "
                    f"WHERE arxiv_id IN ({placeholders})",
                    ids,
                ).fetchall()
                if len(reservations) != len(ids) or any(
                    row["owner_token"] != owner_token for row in reservations
                ):
                    return "id_fetch_reserved"
            conn.execute(
                "UPDATE leases SET expires_at=? WHERE name=? AND owner_token=?",
                (expires_s, lease_name, owner_token),
            )
            conn.execute(
                """
                UPDATE queue_items
                SET claimed_until=?, updated_at=?
                WHERE request_id=? AND status='claimed' AND claimed_by=?
                """,
                (expires_s, current_s, request_id, owner_token),
            )
            if ids:
                conn.execute(
                    f"""
                    UPDATE id_fetch_reservations
                    SET acquired_at=?, expires_at=?
                    WHERE owner_token=? AND arxiv_id IN ({placeholders})
                    """,
                    (current_s, expires_s, owner_token, *ids),
                )
        return None

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

    def status_summary(self, *, now: datetime | None = None) -> BrokerStatus:
        current = ensure_utc(now or utc_now())
        with self.connect() as conn:
            upstream = conn.execute(
                "SELECT COALESCE(SUM(upstream_requests_total), 0) AS total FROM cache_entries"
            ).fetchone()
            rate_totals = conn.execute(
                """
                SELECT
                    COALESCE(SUM(upstream_rate_limited_total), 0) AS rate_limited,
                    COALESCE(SUM(upstream_429_total), 0) AS http_429,
                    COALESCE(SUM(upstream_oai_503_retry_after_total), 0) AS oai_503,
                    COALESCE(SUM(consecutive_rate_limit_total), 0) AS consecutive
                FROM rate_state
                """
            ).fetchone()
            rate = conn.execute(
                """
                SELECT
                    cooldown_until,
                    consecutive_rate_limit_total,
                    upstream_rate_limited_total,
                    upstream_429_total,
                    upstream_oai_503_retry_after_total,
                    last_request_started_at,
                    last_rate_wait_seconds,
                    last_request_latency_ms,
                    last_retry_after_seconds,
                    last_effective_cooldown_seconds,
                    last_rate_limit_kind,
                    last_api_family
                FROM rate_state
                WHERE name = 'arxiv_legacy_api'
                """
            ).fetchone()
            queue = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN status IN ('queued', 'claimed', 'delayed') THEN 1 ELSE 0 END) AS depth,
                    SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) AS ready,
                    SUM(CASE WHEN status = 'delayed' THEN 1 ELSE 0 END) AS delayed,
                    SUM(CASE WHEN status IN ('completed', 'failed') THEN 1 ELSE 0 END) AS terminal,
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
            events = conn.execute("SELECT COUNT(*) AS total FROM events").fetchone()
            sync_jobs = conn.execute(
                f"""
                SELECT
                    COUNT(*) AS total,
                    SUM(
                        CASE
                            WHEN status IN ({_TERMINAL_SYNC_JOB_PLACEHOLDERS})
                                 AND completed_at IS NOT NULL
                            THEN 1
                            ELSE 0
                        END
                    ) AS terminal
                FROM sync_jobs
                """,
                _TERMINAL_SYNC_JOB_STATUSES,
            ).fetchone()
            sync_job_pages = conn.execute(
                "SELECT COUNT(*) AS total FROM sync_job_pages"
            ).fetchone()
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
            upstream_429_total=int(rate_totals["http_429"]),
            upstream_rate_limited_total=int(rate_totals["rate_limited"]),
            upstream_oai_503_retry_after_total=int(rate_totals["oai_503"]),
            consecutive_rate_limit_total=int(rate_totals["consecutive"]),
            cooldown_until=cooldown_until,
            cooldown_active=cooldown_until is not None and cooldown_until > current,
            last_request_started_at=(
                from_isoformat_or_none(rate["last_request_started_at"]) if rate else None
            ),
            last_rate_wait_seconds=(rate["last_rate_wait_seconds"] if rate else None),
            last_request_latency_ms=(rate["last_request_latency_ms"] if rate else None),
            last_retry_after_seconds=(rate["last_retry_after_seconds"] if rate else None),
            last_effective_cooldown_seconds=(
                rate["last_effective_cooldown_seconds"] if rate else None
            ),
            last_rate_limit_kind=(rate["last_rate_limit_kind"] if rate else None),
            last_api_family=(rate["last_api_family"] if rate else None),
            queue_depth_total=int(queue["depth"] or 0),
            queue_ready_total=int(queue["ready"] or 0),
            queue_delayed_total=int(queue["delayed"] or 0),
            queue_items_total=int(queue["total"]),
            queue_terminal_total=int(queue["terminal"] or 0),
            cache_entries_total=int(cache["total"]),
            cache_completed_total=int(cache["completed"] or 0),
            cache_failed_total=int(cache["failed"] or 0),
            papers_total=int(papers["total"]),
            events_total=int(events["total"]),
            sync_jobs_total=int(sync_jobs["total"]),
            sync_jobs_terminal_total=int(sync_jobs["terminal"] or 0),
            sync_job_pages_total=int(sync_job_pages["total"]),
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

    def gc(
        self,
        *,
        cutoff: datetime,
        dry_run: bool = True,
    ) -> RetentionGCResult:
        """Preview or delete old diagnostic and terminal workflow records."""
        self._require_retention_schema()
        normalized_cutoff = ensure_utc(cutoff)
        cutoff_s = isoformat_or_none(normalized_cutoff)
        assert cutoff_s is not None

        if dry_run:
            with self.connect_readonly() as conn:
                counts = self._retention_eligible_counts(conn, cutoff_s)
            return self._retention_result(normalized_cutoff, True, counts)

        with self.begin_immediate() as conn:
            counts = self._retention_eligible_counts(conn, cutoff_s)
            events_deleted = conn.execute(
                "DELETE FROM events WHERE created_at < ?",
                (cutoff_s,),
            ).rowcount
            sync_jobs_deleted = conn.execute(
                f"""
                DELETE FROM sync_jobs
                WHERE sync_job_id IN (
                    SELECT job.sync_job_id
                    FROM sync_jobs AS job
                    WHERE {_RETENTION_SYNC_JOB_FILTER}
                )
                """,
                _retention_sync_job_params(cutoff_s),
            ).rowcount
            queue_items_deleted = conn.execute(
                """
                DELETE FROM queue_items
                WHERE (status = 'completed' AND completed_at IS NOT NULL AND completed_at < ?)
                   OR (status = 'failed' AND updated_at < ?)
                """,
                (cutoff_s, cutoff_s),
            ).rowcount
            conn.execute(
                """
                DELETE FROM upstream_request_budgets AS budget
                WHERE NOT EXISTS (
                    SELECT 1 FROM queue_items AS item
                    WHERE item.upstream_budget_id = budget.budget_id
                )
                  AND NOT EXISTS (
                    SELECT 1 FROM queue_item_upstream_budgets AS membership
                    WHERE membership.budget_id = budget.budget_id
                )
                  AND NOT EXISTS (
                    SELECT 1 FROM sync_jobs AS job
                    WHERE job.upstream_budget_id = budget.budget_id
                )
                """
            )

        deleted = (
            events_deleted,
            queue_items_deleted,
            sync_jobs_deleted,
            counts[3],
        )
        return self._retention_result(normalized_cutoff, False, counts, deleted)

    def vacuum(self) -> StoreVacuumResult:
        """Rewrite an initialized store to reclaim free SQLite pages."""
        self._require_retention_schema()
        storage_bytes_before = self._sqlite_storage_bytes()
        with self.connect() as conn:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            conn.execute("VACUUM")
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        storage_bytes_after = self._sqlite_storage_bytes()
        return StoreVacuumResult(
            database_path=str(self.db_path),
            storage_bytes_before=storage_bytes_before,
            storage_bytes_after=storage_bytes_after,
            reclaimed_bytes=max(0, storage_bytes_before - storage_bytes_after),
        )

    def _require_retention_schema(self) -> None:
        try:
            with self.connect_readonly() as conn:
                rows = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
        except sqlite3.OperationalError as exc:
            raise ValueError(f"Huldra store does not exist or is unreadable: {self.db_path}") from exc
        table_names = {str(row["name"]) for row in rows}
        missing_tables = _RETENTION_TABLES - table_names
        if missing_tables:
            missing = ", ".join(sorted(missing_tables))
            raise ValueError(f"Huldra schema is not initialized; missing tables: {missing}")

    def _sqlite_storage_bytes(self) -> int:
        paths = (
            self.db_path,
            Path(f"{self.db_path}-wal"),
            Path(f"{self.db_path}-shm"),
        )
        return sum(path.stat().st_size for path in paths if path.exists())

    @staticmethod
    def _retention_eligible_counts(
        conn: sqlite3.Connection,
        cutoff: str,
    ) -> tuple[int, int, int, int]:
        row = conn.execute(
            f"""
            WITH eligible_sync_jobs AS (
                SELECT job.sync_job_id
                FROM sync_jobs AS job
                WHERE {_RETENTION_SYNC_JOB_FILTER}
            )
            SELECT
                (SELECT COUNT(*) FROM events WHERE created_at < ?) AS events,
                (
                    SELECT COUNT(*)
                    FROM queue_items
                    WHERE (status = 'completed' AND completed_at IS NOT NULL AND completed_at < ?)
                       OR (status = 'failed' AND updated_at < ?)
                ) AS queue_items,
                (
                    SELECT COUNT(*)
                    FROM eligible_sync_jobs
                ) AS sync_jobs,
                (
                    SELECT COUNT(*)
                    FROM sync_job_pages AS page
                    JOIN eligible_sync_jobs AS job
                      ON job.sync_job_id = page.sync_job_id
                ) AS sync_job_pages
            """,
            (
                *_retention_sync_job_params(cutoff),
                cutoff,
                cutoff,
                cutoff,
            ),
        ).fetchone()
        assert row is not None
        return (
            int(row["events"]),
            int(row["queue_items"]),
            int(row["sync_jobs"]),
            int(row["sync_job_pages"]),
        )

    @staticmethod
    def _retention_result(
        cutoff: datetime,
        dry_run: bool,
        eligible: tuple[int, int, int, int],
        deleted: tuple[int, int, int, int] = (0, 0, 0, 0),
    ) -> RetentionGCResult:
        return RetentionGCResult(
            cutoff=cutoff,
            dry_run=dry_run,
            events_eligible_total=eligible[0],
            queue_items_eligible_total=eligible[1],
            sync_jobs_eligible_total=eligible[2],
            sync_job_pages_eligible_total=eligible[3],
            eligible_total=sum(eligible),
            events_deleted_total=deleted[0],
            queue_items_deleted_total=deleted[1],
            sync_jobs_deleted_total=deleted[2],
            sync_job_pages_deleted_total=deleted[3],
            deleted_total=sum(deleted),
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

    def record_event(self, event_type: str, payload: dict[str, Any]) -> None:
        with self.begin_immediate() as conn:
            self._record_event_conn(conn, event_type, payload)

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


def _oai_request_json(request: OaiHarvestRequest) -> str:
    return request.model_dump_json()


def _oai_request_from_json(value: str) -> OaiHarvestRequest:
    return OaiHarvestRequest.model_validate_json(value)


def _same_oai_resume_scope(left: OaiHarvestRequest, right: OaiHarvestRequest) -> bool:
    return (
        left.metadata_prefix == right.metadata_prefix
        and left.set_spec == right.set_spec
        and left.from_datestamp == right.from_datestamp
        and left.until_datestamp == right.until_datestamp
        and left.mode == right.mode
    )


def _oai_set_key(set_spec: str | None) -> str:
    return set_spec or ""


def _paper_arxiv_ids_in_version_family(conn: sqlite3.Connection, arxiv_id: str) -> tuple[str, ...]:
    base_id = arxiv_id_family_base(arxiv_id)
    rows = conn.execute(
        """
        SELECT arxiv_id FROM papers
        WHERE arxiv_id = ?
           OR arxiv_id LIKE ? ESCAPE '\\'
        """,
        (base_id, f"{_sqlite_like_escape(base_id)}v%"),
    ).fetchall()
    return tuple(
        row["arxiv_id"]
        for row in rows
        if row["arxiv_id"] == base_id or arxiv_id_family_base(row["arxiv_id"]) == base_id
    )


def _oai_paper_for_existing_version_family(
    conn: sqlite3.Connection,
    paper: ArxivPaper,
) -> ArxivPaper:
    arxiv_ids = _paper_arxiv_ids_in_version_family(conn, paper.arxiv_id)
    if not arxiv_ids or paper.arxiv_id in arxiv_ids:
        return paper
    target_arxiv_id = max(arxiv_ids, key=_version_family_merge_preference)
    target_version = arxiv_version(target_arxiv_id)
    update: dict[str, Any] = {
        "arxiv_id": target_arxiv_id,
        "canonical_url": f"https://arxiv.org/abs/{target_arxiv_id}",
    }
    if target_version is not None:
        update["version"] = target_version
    return paper.model_copy(update=update)


def _legacy_paper_for_existing_oai_base_row(
    conn: sqlite3.Connection,
    paper: ArxivPaper,
) -> ArxivPaper:
    if paper.oai_identifier is not None or arxiv_version(paper.arxiv_id) is None:
        return paper
    base_id = arxiv_id_family_base(paper.arxiv_id)
    arxiv_ids = _paper_arxiv_ids_in_version_family(conn, paper.arxiv_id)
    if base_id not in arxiv_ids:
        return paper
    if any(arxiv_version(arxiv_id) is not None for arxiv_id in arxiv_ids):
        return paper
    base_row = conn.execute(
        "SELECT oai_identifier FROM papers WHERE arxiv_id = ?",
        (base_id,),
    ).fetchone()
    if base_row is None or base_row["oai_identifier"] is None:
        return paper
    return paper.model_copy(
        update={
            "arxiv_id": base_id,
            "version": arxiv_version(base_id),
            "canonical_url": f"https://arxiv.org/abs/{base_id}",
        }
    )


def _oai_base_paper_row_for_versioned_read(conn: sqlite3.Connection, arxiv_id: str) -> sqlite3.Row | None:
    if arxiv_version(arxiv_id) is None:
        return None
    base_id = arxiv_id_family_base(arxiv_id)
    if base_id == arxiv_id:
        return None
    return conn.execute(
        """
        SELECT * FROM papers
        WHERE arxiv_id = ?
          AND oai_identifier IS NOT NULL
        """,
        (base_id,),
    ).fetchone()


def _latest_versioned_paper_row_for_base_read(
    conn: sqlite3.Connection,
    arxiv_id: str,
) -> sqlite3.Row | None:
    if arxiv_version(arxiv_id) is not None:
        return None
    base_id = arxiv_id_family_base(arxiv_id)
    rows = conn.execute(
        "SELECT * FROM papers WHERE arxiv_id LIKE ? ESCAPE '\\'",
        (f"{_sqlite_like_escape(base_id)}v%",),
    ).fetchall()
    matching = [
        row
        for row in rows
        if arxiv_version(str(row["arxiv_id"])) is not None
        and arxiv_id_family_base(str(row["arxiv_id"])) == base_id
    ]
    if not matching:
        return None
    return max(
        matching,
        key=lambda row: _version_family_merge_preference(str(row["arxiv_id"])),
    )


def _version_family_merge_preference(arxiv_id: str) -> tuple[int, str]:
    return (arxiv_version(arxiv_id) or -1, arxiv_id)


def _sqlite_like_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _queue_item_upstream_budget_ids_conn(
    conn: sqlite3.Connection,
    request_id: str,
) -> tuple[str, ...]:
    queue_item = conn.execute(
        "SELECT upstream_budget_id FROM queue_items WHERE request_id=?",
        (request_id,),
    ).fetchone()
    if queue_item is None:
        return ()
    memberships = conn.execute(
        """
        SELECT budget_id
        FROM queue_item_upstream_budgets
        WHERE request_id=?
        ORDER BY created_at, budget_id
        """,
        (request_id,),
    ).fetchall()
    budget_ids = [str(row["budget_id"]) for row in memberships]
    primary_budget_id = queue_item["upstream_budget_id"]
    if primary_budget_id is not None:
        budget_ids.insert(0, str(primary_budget_id))
    return tuple(dict.fromkeys(budget_ids))


@dataclass(frozen=True, slots=True)
class _QueueItemBudgetAttemptState:
    attempt_number: int
    unbudgeted_demand: bool
    active_budget_ids: tuple[str, ...]
    uncharged_budget_ids: tuple[str, ...]
    future_budget_ids: tuple[str, ...]


def _queue_item_upstream_budget_attempt_state_conn(
    conn: sqlite3.Connection,
    request_id: str,
) -> _QueueItemBudgetAttemptState:
    queue_item = conn.execute(
        "SELECT attempts_total, unbudgeted_demand FROM queue_items WHERE request_id=?",
        (request_id,),
    ).fetchone()
    if queue_item is None:
        return _QueueItemBudgetAttemptState(0, False, (), (), ())
    attempt_number = int(queue_item["attempts_total"])
    memberships = conn.execute(
        """
        SELECT budget_id, first_attempt_number, last_charged_attempt
        FROM queue_item_upstream_budgets
        WHERE request_id=?
        ORDER BY created_at, budget_id
        """,
        (request_id,),
    ).fetchall()
    active_budget_ids = tuple(
        str(row["budget_id"])
        for row in memberships
        if int(row["first_attempt_number"]) <= attempt_number
    )
    uncharged_budget_ids = tuple(
        str(row["budget_id"])
        for row in memberships
        if int(row["first_attempt_number"]) <= attempt_number
        and int(row["last_charged_attempt"]) < attempt_number
    )
    future_budget_ids = tuple(
        str(row["budget_id"])
        for row in memberships
        if int(row["first_attempt_number"]) > attempt_number
    )
    return _QueueItemBudgetAttemptState(
        attempt_number=attempt_number,
        unbudgeted_demand=bool(queue_item["unbudgeted_demand"]),
        active_budget_ids=active_budget_ids,
        uncharged_budget_ids=uncharged_budget_ids,
        future_budget_ids=future_budget_ids,
    )


def _detach_queue_budget_memberships_conn(
    conn: sqlite3.Connection,
    request_id: str,
    budget_ids: tuple[str, ...],
) -> None:
    if budget_ids:
        placeholders = ",".join("?" for _ in budget_ids)
        conn.execute(
            f"""
            DELETE FROM queue_item_upstream_budgets
            WHERE request_id=? AND budget_id IN ({placeholders})
            """,
            (request_id, *budget_ids),
        )
    replacement = conn.execute(
        """
        SELECT budget_id
        FROM queue_item_upstream_budgets
        WHERE request_id=?
        ORDER BY created_at, budget_id
        LIMIT 1
        """,
        (request_id,),
    ).fetchone()
    conn.execute(
        "UPDATE queue_items SET upstream_budget_id=? WHERE request_id=?",
        (str(replacement["budget_id"]) if replacement is not None else None, request_id),
    )


def _upstream_budget_errors_conn(
    conn: sqlite3.Connection,
    budget_ids: tuple[str, ...],
    *,
    current: datetime,
    enforce_request_limit: bool,
) -> dict[str, str]:
    errors: dict[str, str] = {}
    for budget_id in dict.fromkeys(budget_ids):
        budget = conn.execute(
            """
            SELECT deadline_at, requests_started, max_requests
            FROM upstream_request_budgets
            WHERE budget_id=?
            """,
            (budget_id,),
        ).fetchone()
        if budget is None:
            errors[budget_id] = "request_budget_exceeded"
            continue
        deadline_at = from_isoformat_or_none(budget["deadline_at"])
        if deadline_at is not None and current >= deadline_at:
            errors[budget_id] = "deadline_budget_exceeded"
        elif enforce_request_limit and int(budget["requests_started"]) >= int(
            budget["max_requests"]
        ):
            errors[budget_id] = "request_budget_exceeded"
    return errors


def _preferred_upstream_budget_error(invalid_budgets: dict[str, str]) -> str:
    if "deadline_budget_exceeded" in invalid_budgets.values():
        return "deadline_budget_exceeded"
    return "request_budget_exceeded"


def _check_upstream_budget_deadlines_conn(
    conn: sqlite3.Connection,
    budget_ids: tuple[str, ...],
    *,
    current: datetime,
) -> str | None:
    for budget_id in budget_ids:
        budget = conn.execute(
            "SELECT deadline_at FROM upstream_request_budgets WHERE budget_id=?",
            (budget_id,),
        ).fetchone()
        if budget is None:
            return "request_budget_exceeded"
        deadline_at = from_isoformat_or_none(budget["deadline_at"])
        if deadline_at is not None and current >= deadline_at:
            return "deadline_budget_exceeded"
    return None


@dataclass(frozen=True, slots=True)
class IdReservationResult:
    acquired_ids: tuple[str, ...]
    blocked_until: datetime | None


def _cache_entry_from_row(row: sqlite3.Row) -> CacheEntry:
    return CacheEntry(
        cache_key=row["cache_key"],
        request=_request_from_json(row["request_json"]),
        api_family=row["api_family"],
        status=row["status"],
        requested_at=from_isoformat_or_none(row["requested_at"]),
        completed_at=from_isoformat_or_none(row["completed_at"]),
        refresh_after=from_isoformat_or_none(row["refresh_after"]),
        cooldown_until=from_isoformat_or_none(row["cooldown_until"]),
        upstream_status=row["upstream_status"],
        upstream_requests_total=int(row["upstream_requests_total"]),
        result_count=int(row["result_count"]),
        total_results=row["total_results"],
        coverage_status=row["coverage_status"],
        error_category=row["error_category"],
        error_message=row["error_message"],
    )


def _oai_harvest_state_from_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "harvest_id": row["harvest_id"],
        "request": _oai_request_from_json(row["request_json"]),
        "status": row["status"],
        "records_processed": int(row["records_processed"]),
        "papers_upserted": int(row["papers_upserted"]),
        "deleted_records": int(row["deleted_records"]),
        "pages_total": int(row["pages_total"]),
        "requests_total": int(row["requests_total"]),
        "resumption_token": row["resumption_token"],
        "last_response_date": row["last_response_date"],
        "last_datestamp_seen": row["last_datestamp_seen"],
        "deadline_at": from_isoformat_or_none(row["deadline_at"]),
        "finished_paging": bool(row["finished_paging"]),
        "error_category": row["error_category"],
        "error_message": row["error_message"],
    }


def _queue_item_from_row(row: sqlite3.Row) -> QueueItem:
    return QueueItem(
        request_id=row["request_id"],
        cache_key=row["cache_key"],
        client_id=row["client_id"],
        request=_request_from_json(row["request_json"]),
        priority=int(row["priority"]),
        status=row["status"],
        work_kind=row["work_kind"],
        upstream_budget_id=row["upstream_budget_id"],
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
        authors_detail=json.loads(row["authors_detail_json"]),
        license=row["license"],
        oai_identifier=row["oai_identifier"],
        oai_datestamp=from_isoformat_or_none(row["oai_datestamp"]),
        oai_set_specs=json.loads(row["oai_set_specs_json"]),
        links=json.loads(row["links_json"]),
        versions=json.loads(row["versions_json"]),
        withdrawn=bool(row["withdrawn"]),
        deleted=bool(row["deleted"]),
        raw_metadata=json.loads(row["raw_metadata_json"]),
    )
