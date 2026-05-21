from __future__ import annotations

import sqlite3

from huldra.broker import HuldraBroker
from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.keys import request_cache_key
from huldra.models import ArxivRequest
from tests.conftest import make_paper


def test_completed_cache_with_missing_match_rows_is_not_readable(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI")
    key = request_cache_key(request)
    store.record_completed_cache_entry(cache_key=key, request=request, papers=[make_paper()])
    with store.begin_immediate() as conn:
        conn.execute("DELETE FROM cache_matches WHERE cache_key = ?", (key,))

    result = HuldraBroker(store=store, settings=settings).ensure(request)

    assert result.status == "queued"
    assert result.blocked_reason is None
    events = store.events()
    assert events[-1]["event_type"] == "request_enqueued"
    assert any(
        event["event_type"] == "cache_integrity_failure"
        and event["payload"]["reason"] == "match_count_mismatch"
        for event in events
    )


def test_completed_cache_with_missing_paper_rows_is_not_readable(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI")
    key = request_cache_key(request)
    store.record_completed_cache_entry(cache_key=key, request=request, papers=[make_paper()])
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("DELETE FROM papers")
        conn.commit()

    result = HuldraBroker(store=store, settings=settings).ensure(request)

    assert result.status == "queued"
    assert result.cache_key == key
    assert store.get_readable_completed_cache(key) is None
    assert any(
        event["event_type"] == "cache_integrity_failure"
        and event["payload"]["reason"] == "paper_join_mismatch"
        for event in store.events()
    )


def test_raw_inspection_for_unreadable_cache_omits_readiness_blocked_reason(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI")
    key = request_cache_key(request)
    store.record_completed_cache_entry(cache_key=key, request=request, papers=[make_paper()])
    with store.begin_immediate() as conn:
        conn.execute("DELETE FROM cache_matches WHERE cache_key = ?", (key,))

    result = HuldraBroker(store=store, settings=settings).get_result(key)

    assert result.status == "cache_unreadable"
    assert not result.cache_readable
    assert result.blocked_reason is None
    assert result.error_category is None
