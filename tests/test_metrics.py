from __future__ import annotations

from huldra.db import HuldraStore
from huldra.keys import request_cache_key
from huldra.metrics import collect_status
from huldra.models import ArxivRequest, CoverageStatus, LegacySyncMode
from tests.conftest import make_paper


def test_status_summary_reports_cache_queue_papers_and_events(
    store: HuldraStore,
) -> None:
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI")
    key = request_cache_key(request)
    queued = store.enqueue_request(request, key)
    store.record_completed_cache_entry(
        cache_key=key,
        request=request,
        papers=[make_paper()],
    )
    store.complete_queue_item(queued.request_id)
    sync_job_id = store.create_sync_job(request, LegacySyncMode.SLICE)
    store.record_sync_job_page(
        sync_job_id=sync_job_id,
        request=request,
        cache_key=key,
        status="completed",
    )
    store.complete_sync_job(
        sync_job_id=sync_job_id,
        status="completed",
        coverage_status=CoverageStatus.SLICE,
        result_count=1,
        total_results=1,
        pages_total=1,
        pages_completed_total=1,
    )

    status = collect_status(store)
    assert status.cache_entries_total == 1
    assert status.cache_completed_total == 1
    assert status.papers_total == 1
    assert status.events_total == 4
    assert status.queue_items_total == 1
    assert status.queue_terminal_total == 1
    assert status.sync_jobs_total == 1
    assert status.sync_jobs_terminal_total == 1
    assert status.sync_job_pages_total == 1
    assert {event["event_type"] for event in store.events()} >= {
        "request_enqueued",
        "fetch_success",
    }
