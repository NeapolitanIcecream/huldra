from __future__ import annotations

from huldra.db import HuldraStore
from huldra.keys import request_cache_key
from huldra.metrics import collect_status
from huldra.models import ArxivRequest
from tests.conftest import make_paper


def test_status_summary_reports_cache_queue_papers_and_events(
    store: HuldraStore,
) -> None:
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI")
    key = request_cache_key(request)
    store.enqueue_request(request, key)
    store.record_completed_cache_entry(
        cache_key=key,
        request=request,
        papers=[make_paper()],
    )
    status = collect_status(store)
    assert status.cache_entries_total == 1
    assert status.cache_completed_total == 1
    assert status.papers_total == 1
    assert {event["event_type"] for event in store.events()} >= {
        "request_enqueued",
        "fetch_success",
    }
