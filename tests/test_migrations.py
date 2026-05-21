from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

from huldra.db import HuldraStore


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
        } <= tables
        journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert journal_mode == "wal"
