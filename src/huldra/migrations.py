from __future__ import annotations

import sqlite3

SCHEMA_VERSION = 4


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
            authors_detail_json TEXT NOT NULL DEFAULT '[]',
            license TEXT,
            oai_identifier TEXT,
            oai_datestamp TEXT,
            oai_set_specs_json TEXT NOT NULL DEFAULT '[]',
            links_json TEXT NOT NULL DEFAULT '[]',
            versions_json TEXT NOT NULL DEFAULT '[]',
            withdrawn INTEGER NOT NULL DEFAULT 0,
            deleted INTEGER NOT NULL DEFAULT 0,
            raw_metadata_json TEXT NOT NULL DEFAULT '{}',
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
            coverage_status TEXT NOT NULL DEFAULT 'unknown',
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

        CREATE TABLE IF NOT EXISTS id_fetch_reservations (
            arxiv_id TEXT PRIMARY KEY,
            owner_token TEXT NOT NULL,
            request_id TEXT NOT NULL,
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

        CREATE TABLE IF NOT EXISTS sync_jobs (
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

        CREATE TABLE IF NOT EXISTS sync_job_pages (
            sync_job_id TEXT NOT NULL REFERENCES sync_jobs(sync_job_id) ON DELETE CASCADE,
            cache_key TEXT NOT NULL,
            request_json TEXT NOT NULL,
            start INTEGER NOT NULL,
            max_results INTEGER NOT NULL,
            status TEXT NOT NULL,
            result_count INTEGER NOT NULL DEFAULT 0,
            total_results INTEGER,
            attempt_diagnostics_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (sync_job_id, cache_key)
        );

        CREATE TABLE IF NOT EXISTS oai_harvest_jobs (
            harvest_id TEXT PRIMARY KEY,
            request_json TEXT NOT NULL,
            status TEXT NOT NULL,
            metadata_prefix TEXT NOT NULL,
            set_spec TEXT,
            mode TEXT NOT NULL,
            records_processed INTEGER NOT NULL DEFAULT 0,
            papers_upserted INTEGER NOT NULL DEFAULT 0,
            deleted_records INTEGER NOT NULL DEFAULT 0,
            pages_total INTEGER NOT NULL DEFAULT 0,
            current_watermark TEXT,
            resumption_token TEXT,
            error_category TEXT,
            error_message TEXT,
            started_at TEXT NOT NULL,
            completed_at TEXT
        );

        CREATE TABLE IF NOT EXISTS oai_watermarks (
            metadata_prefix TEXT NOT NULL,
            set_spec TEXT NOT NULL DEFAULT '',
            last_response_date TEXT,
            last_datestamp_seen TEXT,
            last_successful_harvest_id TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (metadata_prefix, set_spec)
        );

        CREATE TABLE IF NOT EXISTS oai_pages (
            harvest_id TEXT NOT NULL REFERENCES oai_harvest_jobs(harvest_id) ON DELETE CASCADE,
            page_index INTEGER NOT NULL,
            request_params_json TEXT NOT NULL,
            resumption_token_hash TEXT,
            status TEXT NOT NULL,
            response_date TEXT,
            records_count INTEGER NOT NULL DEFAULT 0,
            error_category TEXT,
            error_message TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY (harvest_id, page_index)
        );

        CREATE TABLE IF NOT EXISTS oai_records (
            oai_identifier TEXT NOT NULL,
            metadata_prefix TEXT NOT NULL,
            arxiv_id TEXT,
            datestamp TEXT,
            deleted INTEGER NOT NULL DEFAULT 0,
            set_specs_json TEXT NOT NULL DEFAULT '[]',
            raw_xml TEXT,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            PRIMARY KEY (oai_identifier, metadata_prefix)
        );
        """
    )
    _ensure_rate_state_upstream_429_total(conn)
    _ensure_queue_items_work_kind(conn)
    _ensure_column(conn, "cache_entries", "coverage_status", "TEXT NOT NULL DEFAULT 'unknown'")
    _backfill_legacy_cache_coverage_status(conn)
    for column, definition in {
        "authors_detail_json": "TEXT NOT NULL DEFAULT '[]'",
        "license": "TEXT",
        "oai_identifier": "TEXT",
        "oai_datestamp": "TEXT",
        "oai_set_specs_json": "TEXT NOT NULL DEFAULT '[]'",
        "links_json": "TEXT NOT NULL DEFAULT '[]'",
        "versions_json": "TEXT NOT NULL DEFAULT '[]'",
        "withdrawn": "INTEGER NOT NULL DEFAULT 0",
        "deleted": "INTEGER NOT NULL DEFAULT 0",
        "raw_metadata_json": "TEXT NOT NULL DEFAULT '{}'",
    }.items():
        _ensure_column(conn, "papers", column, definition)
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


def _ensure_queue_items_work_kind(conn: sqlite3.Connection) -> None:
    columns = {
        row[1] for row in conn.execute("PRAGMA table_info(queue_items)").fetchall()
    }
    if "work_kind" in columns:
        return
    conn.execute(
        "ALTER TABLE queue_items ADD COLUMN work_kind TEXT NOT NULL DEFAULT 'fetch_missing'"
    )


def _ensure_column(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    definition: str,
) -> None:
    columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column in columns:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def _backfill_legacy_cache_coverage_status(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        UPDATE cache_entries
        SET coverage_status = 'slice'
        WHERE api_family = 'legacy_search'
          AND status = 'completed'
          AND coverage_status = 'unknown'
        """
    )
