from __future__ import annotations

from datetime import UTC, datetime

import pytest

from huldra.keys import request_cache_key
from huldra.models import (
    ArxivRequest,
    ArxivResult,
    HuldraMaintenanceResult,
    HuldraSyncRequest,
    LegacySyncMode,
)


def test_request_requires_client_and_query_or_ids() -> None:
    with pytest.raises(ValueError, match="client_id"):
        ArxivRequest(client_id="", search_query="cat:cs.AI")
    with pytest.raises(ValueError, match="search_query or id_list"):
        ArxivRequest(client_id="demo")


def test_request_limits_single_slice_to_2000_results() -> None:
    with pytest.raises(ValueError):
        ArxivRequest(client_id="demo", search_query="cat:cs.AI", max_results=2001)


def test_legacy_request_rejects_oai_api_family() -> None:
    with pytest.raises(ValueError, match="OaiHarvestRequest"):
        ArxivRequest(client_id="demo", search_query="cat:cs.AI", api_family="oai_pmh")


def test_complete_window_sync_model_requires_wait_true() -> None:
    request = ArxivRequest(client_id="demo", search_query="cat:cs.AI")

    with pytest.raises(ValueError, match="requires wait=True"):
        HuldraSyncRequest(
            requests=[request],
            mode=LegacySyncMode.COMPLETE_WINDOW,
            wait=False,
        )


def test_submitted_window_requires_ordered_utc_pair() -> None:
    with pytest.raises(ValueError, match="provided together"):
        ArxivRequest(
            client_id="demo",
            search_query="cat:cs.AI",
            submitted_start=datetime(2026, 1, 1, tzinfo=UTC),
        )
    with pytest.raises(ValueError, match="before"):
        ArxivRequest(
            client_id="demo",
            search_query="cat:cs.AI",
            submitted_start=datetime(2026, 1, 2, tzinfo=UTC),
            submitted_end=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_submitted_window_rejects_duplicate_date_filter() -> None:
    with pytest.raises(ValueError, match="submittedDate"):
        ArxivRequest(
            client_id="demo",
            search_query="cat:cs.AI AND submittedDate:[202601010000 TO 202601020000]",
            submitted_start=datetime(2026, 1, 1, tzinfo=UTC),
            submitted_end=datetime(2026, 1, 2, tzinfo=UTC),
        )


def test_submitted_window_rejects_non_minute_aligned_bounds() -> None:
    with pytest.raises(ValueError, match="minute precision"):
        ArxivRequest(
            client_id="demo",
            search_query="cat:cs.AI",
            submitted_start=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
            submitted_end=datetime(2026, 1, 2, tzinfo=UTC),
        )


def test_maturity_lag_days_is_not_part_of_cache_key() -> None:
    base = ArxivRequest(
        client_id="demo",
        search_query="cat:cs.AI",
        submitted_start=datetime(2026, 1, 1, tzinfo=UTC),
        submitted_end=datetime(2026, 1, 2, tzinfo=UTC),
    )
    override = base.model_copy(update={"maturity_lag_days": 0})

    assert request_cache_key(base) == request_cache_key(override)


def test_response_models_tolerate_additive_fields() -> None:
    result = ArxivResult.model_validate(
        {
            "status": "queued",
            "cache_key": "huldra:v1:abc",
            "papers_total": 0,
            "future_field": "kept-for-compatibility",
        }
    )

    assert result.status == "queued"
    assert result.model_extra == {"future_field": "kept-for-compatibility"}


def test_maintenance_result_defaults_are_json_friendly() -> None:
    result = HuldraMaintenanceResult()

    assert result.requested_total == 0
    assert result.requests == []
    assert result.model_dump(mode="json")["cooldown_active"] is False
