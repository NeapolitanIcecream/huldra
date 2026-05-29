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
            "rate_state",
            "leases",
            "worker_state",
            "events",
            "sync_jobs",
            "sync_job_pages",
            "oai_harvest_jobs",
            "oai_watermarks",
            "oai_pages",
            "oai_records",
        } <= tables
        rate_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(rate_state)").fetchall()
        }
        assert "upstream_429_total" in rate_columns
        cache_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(cache_entries)").fetchall()
        }
        assert "coverage_status" in cache_columns
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
