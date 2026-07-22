from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

from huldra.db import HuldraStore
from huldra.migrations import apply_migrations
from huldra.models import ArxivRequest


def test_migrations_are_idempotent_and_create_required_tables(tmp_path: Path) -> None:
    db = tmp_path / "huldra.db"
    store = HuldraStore(db)
    store.init_schema()
    store.init_schema()
    assert db.exists()
    with closing(sqlite3.connect(db)) as conn:
        tables = {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }
        assert {
            "papers",
            "cache_entries",
            "cache_matches",
            "queue_items",
            "queue_item_upstream_budgets",
            "rate_state",
            "leases",
            "worker_state",
            "events",
            "sync_jobs",
            "sync_job_pages",
            "upstream_request_budgets",
            "oai_harvest_jobs",
            "oai_watermarks",
            "oai_pages",
            "oai_records",
        } <= tables
        rate_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(rate_state)").fetchall()
        }
        assert "upstream_429_total" in rate_columns
        assert {
            "consecutive_rate_limit_total",
            "upstream_rate_limited_total",
            "upstream_oai_503_retry_after_total",
            "last_request_started_at",
            "last_rate_wait_seconds",
            "last_request_latency_ms",
            "last_retry_after_seconds",
            "last_effective_cooldown_seconds",
            "last_rate_limit_kind",
            "last_api_family",
        } <= rate_columns
        cache_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(cache_entries)").fetchall()
        }
        assert {"coverage_status", "refresh_after"} <= cache_columns
        queue_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(queue_items)").fetchall()
        }
        assert {"upstream_budget_id", "upstream_budget_gate_closed"} <= queue_columns
        sync_job_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(sync_jobs)").fetchall()
        }
        assert "upstream_budget_id" in sync_job_columns
        oai_job_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(oai_harvest_jobs)").fetchall()
        }
        assert {
            "requests_total",
            "last_response_date",
            "last_datestamp_seen",
            "deadline_at",
            "finished_paging",
            "updated_at",
        } <= oai_job_columns
        oai_page_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(oai_pages)").fetchall()
        }
        assert "next_resumption_token_hash" in oai_page_columns
        paper_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(papers)").fetchall()
        }
        assert {"authors_detail_json", "license", "oai_identifier", "deleted"} <= paper_columns
        journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert journal_mode == "wal"


def test_migration_backfills_completed_legacy_cache_entries_as_slices(tmp_path: Path) -> None:
    db = tmp_path / "old.db"
    request = ArxivRequest(client_id="legacy", search_query="cat:cs.AI")
    with closing(sqlite3.connect(db)) as conn:
        conn.executescript(
            """
            CREATE TABLE cache_entries (
                cache_key TEXT PRIMARY KEY,
                request_json TEXT NOT NULL,
                api_family TEXT NOT NULL,
                status TEXT NOT NULL,
                requested_at TEXT,
                completed_at TEXT,
                cooldown_until TEXT,
                upstream_status INTEGER,
                upstream_requests_total INTEGER NOT NULL DEFAULT 0,
                result_count INTEGER NOT NULL DEFAULT 0,
                total_results INTEGER,
                error_category TEXT,
                error_message TEXT
            );
            """
        )
        conn.execute(
            """
            INSERT INTO cache_entries(
                cache_key, request_json, api_family, status,
                upstream_requests_total, result_count, total_results
            )
            VALUES (?, ?, 'legacy_search', 'completed', 1, 1, 3)
            """,
            ("old-completed", request.model_dump_json()),
        )
        conn.execute(
            """
            INSERT INTO cache_entries(
                cache_key, request_json, api_family, status,
                upstream_requests_total, result_count, total_results
            )
            VALUES (?, ?, 'legacy_search', 'failed', 1, 0, NULL)
            """,
            ("old-failed", request.model_dump_json()),
        )

        apply_migrations(conn)

        rows = dict(
            conn.execute(
                "SELECT cache_key, coverage_status FROM cache_entries ORDER BY cache_key"
            ).fetchall()
        )

    assert rows == {"old-completed": "slice", "old-failed": "unknown"}


def test_migration_adds_durable_upstream_budget_to_existing_workflow_tables(
    tmp_path: Path,
) -> None:
    db = tmp_path / "old-workflow.db"
    request = ArxivRequest(client_id="legacy", search_query="cat:cs.AI")
    with closing(sqlite3.connect(db)) as conn:
        conn.executescript(
            """
            CREATE TABLE queue_items (
                request_id TEXT PRIMARY KEY,
                cache_key TEXT NOT NULL,
                client_id TEXT NOT NULL,
                request_json TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                work_kind TEXT NOT NULL DEFAULT 'fetch_missing',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                claimed_by TEXT,
                claimed_until TEXT,
                attempts_total INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT,
                completed_at TEXT,
                error_category TEXT,
                error_message TEXT
            );
            CREATE TABLE sync_jobs (
                sync_job_id TEXT PRIMARY KEY,
                mode TEXT NOT NULL,
                request_json TEXT NOT NULL,
                status TEXT NOT NULL,
                coverage_status TEXT NOT NULL DEFAULT 'unknown',
                result_count INTEGER NOT NULL DEFAULT 0,
                total_results INTEGER,
                pages_total INTEGER NOT NULL DEFAULT 0,
                pages_completed_total INTEGER NOT NULL DEFAULT 0,
                error_category TEXT,
                error_message TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT
            );
            """
        )
        conn.execute(
            """
            INSERT INTO queue_items(
                request_id, cache_key, client_id, request_json,
                status, created_at, updated_at
            )
            VALUES ('legacy-request', 'legacy-key', 'legacy', ?,
                    'queued', '2026-01-01T00:00:00+00:00',
                    '2026-01-01T00:00:00+00:00')
            """,
            (request.model_dump_json(),),
        )

        apply_migrations(conn)

        queue_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(queue_items)").fetchall()
        }
        sync_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(sync_jobs)").fetchall()
        }
        budget_table = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='upstream_request_budgets'"
        ).fetchone()
        index = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='index' AND name='idx_queue_upstream_budget'"
        ).fetchone()
        membership_table = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='queue_item_upstream_budgets'"
        ).fetchone()
        membership_index = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='index' AND name='idx_queue_item_upstream_budgets_budget'"
        ).fetchone()
        membership_columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(queue_item_upstream_budgets)"
            ).fetchall()
        }
        version = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0]
        unbudgeted_demand = conn.execute(
            "SELECT unbudgeted_demand FROM queue_items WHERE request_id='legacy-request'"
        ).fetchone()[0]

    assert {"upstream_budget_id", "unbudgeted_demand"} <= queue_columns
    assert "upstream_budget_id" in sync_columns
    assert budget_table == ("upstream_request_budgets",)
    assert index == ("idx_queue_upstream_budget",)
    assert membership_table == ("queue_item_upstream_budgets",)
    assert membership_index == ("idx_queue_item_upstream_budgets_budget",)
    assert {"first_attempt_number", "last_charged_attempt"} <= membership_columns
    assert unbudgeted_demand == 1
    assert version == 8


def test_migration_backfills_primary_queue_budget_membership(tmp_path: Path) -> None:
    db = tmp_path / "v7.db"
    store = HuldraStore(db)
    store.init_schema()
    budget_id = store.create_upstream_request_budget(max_requests=2, deadline_at=None)
    item, joined = store.enqueue_request_for_work(
        ArxivRequest(client_id="legacy", search_query="cat:cs.AI"),
        upstream_budget_id=budget_id,
    )
    assert not joined
    with store.begin_immediate() as conn:
        conn.execute(
            "DELETE FROM queue_item_upstream_budgets WHERE request_id=?",
            (item.request_id,),
        )

    with closing(sqlite3.connect(db)) as conn:
        apply_migrations(conn)
        memberships = conn.execute(
            """
            SELECT request_id, budget_id
            FROM queue_item_upstream_budgets
            WHERE request_id=?
            """,
            (item.request_id,),
        ).fetchall()

    assert memberships == [(item.request_id, budget_id)]


def test_migration_preserves_legacy_429_counts_as_rate_limit_counts(tmp_path: Path) -> None:
    db = tmp_path / "old-rate-state.db"
    with closing(sqlite3.connect(db)) as conn:
        conn.executescript(
            """
            CREATE TABLE rate_state (
                name TEXT PRIMARY KEY,
                last_request_at TEXT,
                cooldown_until TEXT,
                consecutive_429_total INTEGER NOT NULL DEFAULT 0,
                last_status INTEGER,
                last_error_message TEXT
            );
            INSERT INTO rate_state(name, consecutive_429_total, last_status)
            VALUES ('arxiv_legacy_api', 2, 429);
            """
        )

        apply_migrations(conn)

        row = conn.execute(
            """
            SELECT upstream_429_total, consecutive_rate_limit_total,
                   upstream_rate_limited_total
            FROM rate_state
            WHERE name = 'arxiv_legacy_api'
            """
        ).fetchone()

    assert row == (2, 2, 2)
