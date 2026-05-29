from __future__ import annotations

from datetime import UTC, datetime, timedelta

from huldra.db import HuldraStore
from huldra.keys import request_cache_key
from huldra.models import ArxivRequest, OaiRecord, RateState, RequestStatus
from huldra.time import utc_now
from tests.conftest import make_paper


def test_store_records_completed_cache_and_reads_ordered_papers(
    store: HuldraStore,
) -> None:
    request = ArxivRequest(client_id="demo", id_list=("2401.00001",))
    key = request_cache_key(request)
    store.record_completed_cache_entry(
        cache_key=key,
        request=request,
        papers=[make_paper("2401.00001v1")],
        total_results=1,
    )
    entry = store.get_cache_entry(key)
    papers = store.get_cached_papers(key)
    assert entry is not None
    assert entry.status == "completed"
    assert entry.total_results == 1
    assert [paper.arxiv_id for paper in papers] == ["2401.00001v1"]


def test_upsert_papers_preserves_oai_provenance_from_legacy_refresh(
    store: HuldraStore,
) -> None:
    oai_datestamp = datetime(2026, 5, 27, tzinfo=UTC)
    oai_paper = make_paper("2401.00001v1").model_copy(
        update={
            "title": "OAI Paper",
            "oai_identifier": "oai:arXiv.org:2401.00001",
            "oai_datestamp": oai_datestamp,
            "oai_set_specs": ["cs:cs:AI"],
        }
    )
    legacy_refresh = make_paper("2401.00001v1").model_copy(update={"title": "Legacy Refresh"})

    store.upsert_papers([oai_paper])
    store.upsert_papers([legacy_refresh])

    paper = store.get_paper("2401.00001v1")
    assert paper is not None
    assert paper.title == "Legacy Refresh"
    assert paper.oai_identifier == "oai:arXiv.org:2401.00001"
    assert paper.oai_datestamp == oai_datestamp
    assert paper.oai_set_specs == ["cs:cs:AI"]


def test_upsert_papers_allows_oai_refresh_to_update_oai_provenance(
    store: HuldraStore,
) -> None:
    first_datestamp = datetime(2026, 5, 27, tzinfo=UTC)
    refreshed_datestamp = datetime(2026, 5, 28, tzinfo=UTC)
    first_oai_paper = make_paper("2401.00001v1").model_copy(
        update={
            "oai_identifier": "oai:arXiv.org:2401.00001",
            "oai_datestamp": first_datestamp,
            "oai_set_specs": ["cs:cs:AI"],
        }
    )
    refreshed_oai_paper = make_paper("2401.00001v1").model_copy(
        update={
            "oai_identifier": "oai:arXiv.org:2401.00001",
            "oai_datestamp": refreshed_datestamp,
            "oai_set_specs": [],
        }
    )

    store.upsert_papers([first_oai_paper])
    store.upsert_papers([refreshed_oai_paper])

    paper = store.get_paper("2401.00001v1")
    assert paper is not None
    assert paper.oai_identifier == "oai:arXiv.org:2401.00001"
    assert paper.oai_datestamp == refreshed_datestamp
    assert paper.oai_set_specs == []


def test_upsert_papers_preserves_oai_tombstone_from_legacy_refresh(
    store: HuldraStore,
) -> None:
    datestamp = datetime(2026, 5, 28, tzinfo=UTC)
    legacy_paper = make_paper("2401.00002v1")
    legacy_refresh = make_paper("2401.00002v1").model_copy(update={"title": "Legacy Refresh"})

    store.upsert_papers([legacy_paper])
    store.upsert_oai_records(
        [
            OaiRecord(
                oai_identifier="oai:arXiv.org:2401.00002",
                arxiv_id="2401.00002",
                metadata_prefix="arXiv",
                datestamp=datestamp,
                set_specs=["cs:cs:AI"],
                deleted=True,
            )
        ]
    )
    store.upsert_papers([legacy_refresh])

    paper = store.get_paper("2401.00002v1")
    assert paper is not None
    assert paper.title == "Legacy Refresh"
    assert paper.deleted
    assert paper.oai_identifier == "oai:arXiv.org:2401.00002"
    assert paper.oai_datestamp == datestamp
    assert paper.oai_set_specs == ["cs:cs:AI"]


def test_upsert_papers_allows_oai_refresh_to_clear_oai_tombstone(
    store: HuldraStore,
) -> None:
    tombstone_datestamp = datetime(2026, 5, 28, tzinfo=UTC)
    refreshed_datestamp = datetime(2026, 5, 29, tzinfo=UTC)
    legacy_paper = make_paper("2401.00003v1")
    oai_refresh = make_paper("2401.00003v1").model_copy(
        update={
            "title": "OAI Refresh",
            "oai_identifier": "oai:arXiv.org:2401.00003",
            "oai_datestamp": refreshed_datestamp,
            "oai_set_specs": ["cs:cs:AI"],
        }
    )

    store.upsert_papers([legacy_paper])
    store.upsert_oai_records(
        [
            OaiRecord(
                oai_identifier="oai:arXiv.org:2401.00003",
                arxiv_id="2401.00003",
                metadata_prefix="arXiv",
                datestamp=tombstone_datestamp,
                set_specs=["cs:cs:AI"],
                deleted=True,
            )
        ]
    )
    store.upsert_papers([oai_refresh])

    paper = store.get_paper("2401.00003v1")
    assert paper is not None
    assert paper.title == "OAI Refresh"
    assert not paper.deleted
    assert paper.oai_identifier == "oai:arXiv.org:2401.00003"
    assert paper.oai_datestamp == refreshed_datestamp


def test_upsert_oai_deleted_record_marks_versioned_legacy_paper(
    store: HuldraStore,
) -> None:
    legacy_paper = make_paper("2401.00002v1")
    unrelated_paper = make_paper("2401.000020v1")
    datestamp = datetime(2026, 5, 28, tzinfo=UTC)

    store.upsert_papers([legacy_paper, unrelated_paper])
    store.upsert_oai_records(
        [
            OaiRecord(
                oai_identifier="oai:arXiv.org:2401.00002",
                arxiv_id="2401.00002",
                metadata_prefix="arXiv",
                datestamp=datestamp,
                set_specs=["cs:cs:AI"],
                deleted=True,
            )
        ]
    )

    deleted = store.get_paper("2401.00002v1")
    unrelated = store.get_paper("2401.000020v1")

    assert deleted is not None
    assert deleted.deleted
    assert deleted.oai_identifier == "oai:arXiv.org:2401.00002"
    assert deleted.oai_datestamp == datestamp
    assert deleted.oai_set_specs == ["cs:cs:AI"]
    assert unrelated is not None
    assert not unrelated.deleted


def test_upsert_oai_record_merges_non_deleted_paper_into_versioned_legacy_row(
    store: HuldraStore,
) -> None:
    legacy_paper = make_paper("2401.00004v1")
    datestamp = datetime(2026, 5, 28, tzinfo=UTC)
    oai_paper = make_paper("2401.00004").model_copy(
        update={
            "title": "OAI Refresh",
            "oai_identifier": "oai:arXiv.org:2401.00004",
            "oai_datestamp": datestamp,
            "oai_set_specs": ["cs:cs:AI"],
        }
    )

    store.upsert_papers([legacy_paper])
    result = store.upsert_oai_records(
        [
            OaiRecord(
                oai_identifier="oai:arXiv.org:2401.00004",
                arxiv_id="2401.00004",
                metadata_prefix="arXiv",
                datestamp=datestamp,
                set_specs=["cs:cs:AI"],
                paper=oai_paper,
            )
        ]
    )

    paper = store.get_paper("2401.00004v1")
    duplicate = store.get_paper("2401.00004")
    with store.connect() as conn:
        family_count = conn.execute(
            "SELECT COUNT(*) FROM papers WHERE arxiv_id IN ('2401.00004', '2401.00004v1')"
        ).fetchone()[0]

    assert result == (1, 1, 0)
    assert paper is not None
    assert paper.arxiv_id == "2401.00004v1"
    assert paper.version == 1
    assert paper.canonical_url == "https://arxiv.org/abs/2401.00004v1"
    assert paper.title == "OAI Refresh"
    assert paper.oai_identifier == "oai:arXiv.org:2401.00004"
    assert paper.oai_datestamp == datestamp
    assert paper.oai_set_specs == ["cs:cs:AI"]
    assert duplicate is None
    assert family_count == 1


def test_legacy_versioned_cache_write_merges_into_existing_oai_base_row(
    store: HuldraStore,
) -> None:
    datestamp = datetime(2026, 5, 28, tzinfo=UTC)
    oai_paper = make_paper("2401.00005").model_copy(
        update={
            "version": None,
            "title": "OAI Base",
            "oai_identifier": "oai:arXiv.org:2401.00005",
            "oai_datestamp": datestamp,
            "oai_set_specs": ["cs:cs:AI"],
        }
    )
    legacy_refresh = make_paper("2401.00005v1").model_copy(update={"title": "Legacy Refresh"})
    request = ArxivRequest(client_id="demo", id_list=("2401.00005v1",))
    key = request_cache_key(request)

    store.upsert_oai_records(
        [
            OaiRecord(
                oai_identifier="oai:arXiv.org:2401.00005",
                arxiv_id="2401.00005",
                metadata_prefix="arXiv",
                datestamp=datestamp,
                set_specs=["cs:cs:AI"],
                paper=oai_paper,
            )
        ]
    )
    store.record_completed_cache_entry(
        cache_key=key,
        request=request,
        papers=[legacy_refresh],
        total_results=1,
    )

    paper = store.get_paper("2401.00005")
    alias = store.get_paper("2401.00005v1")
    cached_papers = store.get_cached_papers(key)
    with store.connect() as conn:
        family_count = conn.execute(
            "SELECT COUNT(*) FROM papers WHERE arxiv_id IN ('2401.00005', '2401.00005v1')"
        ).fetchone()[0]
        versioned_row = conn.execute("SELECT arxiv_id FROM papers WHERE arxiv_id = '2401.00005v1'").fetchone()
        cache_match = conn.execute(
            "SELECT arxiv_id FROM cache_matches WHERE cache_key = ?",
            (key,),
        ).fetchone()

    assert paper is not None
    assert paper.arxiv_id == "2401.00005"
    assert paper.version is None
    assert paper.title == "Legacy Refresh"
    assert paper.oai_identifier == "oai:arXiv.org:2401.00005"
    assert paper.oai_datestamp == datestamp
    assert paper.oai_set_specs == ["cs:cs:AI"]
    assert alias is not None
    assert alias.arxiv_id == "2401.00005"
    assert versioned_row is None
    assert family_count == 1
    assert cache_match["arxiv_id"] == "2401.00005"
    assert [cached.arxiv_id for cached in cached_papers] == ["2401.00005"]


def test_legacy_versioned_upsert_preserves_existing_oai_base_tombstone(
    store: HuldraStore,
) -> None:
    datestamp = datetime(2026, 5, 28, tzinfo=UTC)
    oai_paper = make_paper("2401.00006").model_copy(
        update={
            "version": None,
            "oai_identifier": "oai:arXiv.org:2401.00006",
            "oai_datestamp": datestamp,
            "oai_set_specs": ["cs:cs:AI"],
        }
    )
    legacy_refresh = make_paper("2401.00006v1").model_copy(update={"title": "Legacy Refresh"})

    store.upsert_oai_records(
        [
            OaiRecord(
                oai_identifier="oai:arXiv.org:2401.00006",
                arxiv_id="2401.00006",
                metadata_prefix="arXiv",
                datestamp=datestamp,
                set_specs=["cs:cs:AI"],
                paper=oai_paper,
            )
        ]
    )
    store.upsert_oai_records(
        [
            OaiRecord(
                oai_identifier="oai:arXiv.org:2401.00006",
                arxiv_id="2401.00006",
                metadata_prefix="arXiv",
                datestamp=datestamp,
                set_specs=["cs:cs:AI"],
                deleted=True,
            )
        ]
    )
    store.upsert_papers([legacy_refresh])

    paper = store.get_paper("2401.00006")
    alias = store.get_paper("2401.00006v1")
    with store.connect() as conn:
        versioned_row = conn.execute("SELECT arxiv_id FROM papers WHERE arxiv_id = '2401.00006v1'").fetchone()

    assert paper is not None
    assert paper.arxiv_id == "2401.00006"
    assert paper.title == "Legacy Refresh"
    assert paper.deleted
    assert paper.oai_identifier == "oai:arXiv.org:2401.00006"
    assert paper.oai_datestamp == datestamp
    assert paper.oai_set_specs == ["cs:cs:AI"]
    assert alias is not None
    assert alias.arxiv_id == "2401.00006"
    assert versioned_row is None


def test_legacy_versioned_upsert_keeps_legacy_only_family_rows_distinct(
    store: HuldraStore,
) -> None:
    base_paper = make_paper("2401.00007").model_copy(update={"version": None})
    versioned_paper = make_paper("2401.00007v1")

    store.upsert_papers([base_paper])
    store.upsert_papers([versioned_paper])

    assert store.get_paper("2401.00007") is not None
    assert store.get_paper("2401.00007v1") is not None


def test_versioned_read_resolves_to_oai_base_row(
    store: HuldraStore,
) -> None:
    datestamp = datetime(2026, 5, 28, tzinfo=UTC)
    oai_paper = make_paper("2401.00008").model_copy(
        update={
            "version": None,
            "title": "OAI Base",
            "oai_identifier": "oai:arXiv.org:2401.00008",
            "oai_datestamp": datestamp,
            "oai_set_specs": ["cs:cs:AI"],
        }
    )

    store.upsert_oai_records(
        [
            OaiRecord(
                oai_identifier="oai:arXiv.org:2401.00008",
                arxiv_id="2401.00008",
                metadata_prefix="arXiv",
                datestamp=datestamp,
                set_specs=["cs:cs:AI"],
                paper=oai_paper,
            )
        ]
    )

    paper = store.get_paper("2401.00008v1")
    papers_by_id = store.get_papers_by_ids(("2401.00008v1",))

    assert paper is not None
    assert paper.arxiv_id == "2401.00008"
    assert paper.title == "OAI Base"
    assert papers_by_id["2401.00008v1"].arxiv_id == "2401.00008"


def test_versioned_read_prefers_exact_row_over_oai_base_alias(
    store: HuldraStore,
) -> None:
    datestamp = datetime(2026, 5, 28, tzinfo=UTC)
    base_paper = make_paper("2401.00009").model_copy(update={"version": None, "title": "OAI Base"})
    versioned_paper = make_paper("2401.00009v1").model_copy(update={"title": "Exact Version"})

    store.upsert_papers([base_paper, versioned_paper])
    with store.begin_immediate() as conn:
        conn.execute(
            """
            UPDATE papers
            SET oai_identifier=?,
                oai_datestamp=?,
                oai_set_specs_json=?
            WHERE arxiv_id=?
            """,
            ("oai:arXiv.org:2401.00009", datestamp.isoformat(), '["cs:cs:AI"]', "2401.00009"),
        )

    paper = store.get_paper("2401.00009v1")
    papers_by_id = store.get_papers_by_ids(("2401.00009v1",))

    assert paper is not None
    assert paper.arxiv_id == "2401.00009v1"
    assert paper.title == "Exact Version"
    assert papers_by_id["2401.00009v1"].arxiv_id == "2401.00009v1"


def test_enqueue_dedupes_pending_cache_key(store: HuldraStore) -> None:
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI")
    first = store.enqueue_request(request)
    second = store.enqueue_request(request)
    assert second.request_id == first.request_id


def test_claim_next_queue_item_is_exclusive_until_claim_expires(
    store: HuldraStore,
) -> None:
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI")
    item = store.enqueue_request(request)
    first = store.claim_next_queue_item(owner_token="w1", claim_timeout_seconds=60)
    second = store.claim_next_queue_item(owner_token="w2", claim_timeout_seconds=60)
    assert first is not None
    assert first.request_id == item.request_id
    assert second is None
    stale_time = utc_now() + timedelta(seconds=61)
    recovered = store.claim_next_queue_item(
        owner_token="w2",
        claim_timeout_seconds=60,
        now=stale_time,
    )
    assert recovered is not None
    assert recovered.request_id == item.request_id
    assert recovered.claimed_by == "w2"


def test_rate_state_and_leases_are_durable(store: HuldraStore) -> None:
    cooldown = datetime(2026, 1, 1, tzinfo=UTC)
    store.set_rate_state(
        RateState(
            name="arxiv_legacy_api",
            cooldown_until=cooldown,
            consecutive_429_total=2,
            last_status=429,
        )
    )
    assert store.get_rate_state().cooldown_until == cooldown
    assert store.acquire_lease("upstream_fetch", "w1", 60)
    assert not store.acquire_lease("upstream_fetch", "w2", 60)
    store.release_lease("upstream_fetch", "w1")
    assert store.acquire_lease("upstream_fetch", "w2", 60)


def test_release_or_delay_marks_queue_item_failed(store: HuldraStore) -> None:
    item = store.enqueue_request(ArxivRequest(client_id="demo", search_query="cat:cs.AI"))
    store.release_or_delay_queue_item(
        item.request_id,
        status=RequestStatus.FAILED,
        error_category="non_retryable",
        error_message="bad request",
    )
    updated = store.get_queue_item(item.request_id)
    assert updated is not None
    assert updated.status == RequestStatus.FAILED
    assert updated.error_category == "non_retryable"
