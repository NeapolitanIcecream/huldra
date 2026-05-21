from __future__ import annotations

import sqlite3

SCHEMA_VERSION = 2


def apply_migrations(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS papers (
            arxiv_id TEXT PRIMARY KEY,
            version INTEGER,
            canonical_url TEXT NOT NULL,
            title TEXT NOT NULL,
            abstract TEXT,
            authors_json TEXT NOT NULL,
            primary_category TEXT,
            categories_json TEXT NOT NULL,
            published_at TEXT,
            updated_at TEXT,
            comment TEXT,
            journal_ref TEXT,
            doi TEXT,
            raw_atom_json TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS cache_entries (
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

        CREATE TABLE IF NOT EXISTS cache_matches (
            cache_key TEXT NOT NULL REFERENCES cache_entries(cache_key) ON DELETE CASCADE,
            arxiv_id TEXT NOT NULL REFERENCES papers(arxiv_id) ON DELETE CASCADE,
            sort_position INTEGER NOT NULL,
            matched_at TEXT NOT NULL,
            PRIMARY KEY (cache_key, arxiv_id)
        );

        CREATE TABLE IF NOT EXISTS queue_items (
            request_id TEXT PRIMARY KEY,
            cache_key TEXT NOT NULL,
            client_id TEXT NOT NULL,
            request_json TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
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

        CREATE UNIQUE INDEX IF NOT EXISTS idx_queue_pending_cache_key
            ON queue_items(cache_key)
            WHERE status IN ('queued', 'delayed', 'claimed');

        CREATE INDEX IF NOT EXISTS idx_queue_ready
            ON queue_items(status, next_attempt_at, priority, created_at);

        CREATE TABLE IF NOT EXISTS rate_state (
            name TEXT PRIMARY KEY,
            last_request_at TEXT,
            cooldown_until TEXT,
            consecutive_429_total INTEGER NOT NULL DEFAULT 0,
            upstream_429_total INTEGER NOT NULL DEFAULT 0,
            last_status INTEGER,
            last_error_message TEXT
        );

        CREATE TABLE IF NOT EXISTS leases (
            name TEXT PRIMARY KEY,
            owner_token TEXT NOT NULL,
            acquired_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS worker_state (
            name TEXT PRIMARY KEY,
            last_started_at TEXT,
            last_heartbeat_at TEXT,
            last_completed_at TEXT,
            next_wake_at TEXT,
            last_error_category TEXT,
            last_error_message TEXT
        );

        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """
    )
    _ensure_rate_state_upstream_429_total(conn)
    conn.execute(
        "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (1, datetime('now'))"
    )
    conn.execute(
        "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, datetime('now'))",
        (SCHEMA_VERSION,),
    )
    conn.commit()


def _ensure_rate_state_upstream_429_total(conn: sqlite3.Connection) -> None:
    columns = {
        row[1] for row in conn.execute("PRAGMA table_info(rate_state)").fetchall()
    }
    if "upstream_429_total" in columns:
        return
    conn.execute(
        "ALTER TABLE rate_state ADD COLUMN upstream_429_total INTEGER NOT NULL DEFAULT 0"
    )
    conn.execute(
        """
        UPDATE rate_state
        SET upstream_429_total = MAX(upstream_429_total, consecutive_429_total)
        """
    )
