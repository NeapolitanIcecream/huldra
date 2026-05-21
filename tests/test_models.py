from __future__ import annotations

from datetime import UTC, datetime

import pytest

from huldra.models import ArxivRequest


def test_request_requires_client_and_query_or_ids() -> None:
    with pytest.raises(ValueError, match="client_id"):
        ArxivRequest(client_id="", search_query="cat:cs.AI")
    with pytest.raises(ValueError, match="search_query or id_list"):
        ArxivRequest(client_id="demo")


def test_request_limits_single_slice_to_2000_results() -> None:
    with pytest.raises(ValueError):
        ArxivRequest(client_id="demo", search_query="cat:cs.AI", max_results=2001)


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
