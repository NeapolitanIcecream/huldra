from __future__ import annotations

from datetime import UTC, datetime

import pytest

from huldra.keys import build_arxiv_api_params, normalize_arxiv_id, request_cache_key
from huldra.models import ArxivRequest


def test_equivalent_search_whitespace_has_same_cache_key() -> None:
    r1 = ArxivRequest(client_id="a", search_query="cat:cs.AI   AND all:agent")
    r2 = ArxivRequest(client_id="b", search_query=" cat:cs.AI AND all:agent ")
    assert request_cache_key(r1) == request_cache_key(r2)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("arXiv:2401.00001v2", "2401.00001v2"),
        ("https://arxiv.org/abs/2401.00001v2", "2401.00001v2"),
        ("https://arxiv.org/pdf/2401.00001v2.pdf", "2401.00001v2"),
        ("abs/math/0309136v1", "math/0309136v1"),
    ],
)
def test_normalize_arxiv_id_preserves_version(raw: str, expected: str) -> None:
    assert normalize_arxiv_id(raw) == expected


def test_id_order_is_part_of_cache_key() -> None:
    r1 = ArxivRequest(client_id="a", id_list=("2401.00001", "2401.00002"))
    r2 = ArxivRequest(client_id="a", id_list=("2401.00002", "2401.00001"))
    assert request_cache_key(r1) != request_cache_key(r2)


def test_build_params_adds_submitted_date_filter() -> None:
    request = ArxivRequest(
        client_id="a",
        search_query="cat:cs.AI",
        submitted_start=datetime(2026, 1, 1, tzinfo=UTC),
        submitted_end=datetime(2026, 1, 2, tzinfo=UTC),
    )
    params = build_arxiv_api_params(request)
    assert params["search_query"] == ("(cat:cs.AI) AND submittedDate:[202601010000 TO 202601012359]")
