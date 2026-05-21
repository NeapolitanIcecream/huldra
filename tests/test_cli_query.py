from __future__ import annotations

import json

from typer.testing import CliRunner

from huldra.cli import app
from huldra.db import HuldraStore
from huldra.keys import request_cache_key
from huldra.models import ArxivRequest
from tests.conftest import make_paper


def test_query_result_and_paper_cli_do_not_fetch_upstream(tmp_path) -> None:  # type: ignore[no-untyped-def]
    db = tmp_path / "huldra.db"
    runner = CliRunner()
    query = runner.invoke(
        app,
        [
            "query",
            "--db",
            str(db),
            "--client-id",
            "smoke",
            "--search-query",
            "cat:cs.AI",
            "--max-results",
            "1",
            "--json",
        ],
    )
    assert query.exit_code == 0
    payload = json.loads(query.output)
    assert payload["status"] == "queued"
    result = runner.invoke(
        app,
        ["result", "--db", str(db), "--cache-key", payload["cache_key"], "--json"],
    )
    paper = runner.invoke(
        app,
        ["paper", "--db", str(db), "--arxiv-id", "2401.00001", "--json"],
    )
    assert result.exit_code == 0
    assert json.loads(result.output)["status"] == "cache_miss"
    assert paper.exit_code == 0
    assert json.loads(paper.output) is None


def test_paper_cli_reads_old_style_arxiv_id(tmp_path) -> None:  # type: ignore[no-untyped-def]
    db = tmp_path / "huldra.db"
    store = HuldraStore(db)
    store.init_schema()
    request = ArxivRequest(client_id="cli", id_list=("hep-th/9901001v1",))
    store.record_completed_cache_entry(
        cache_key=request_cache_key(request),
        request=request,
        papers=[make_paper("hep-th/9901001v1")],
    )

    result = CliRunner().invoke(
        app,
        ["paper", "--db", str(db), "--arxiv-id", "hep-th/9901001v1", "--json"],
    )

    assert result.exit_code == 0
    assert json.loads(result.output)["arxiv_id"] == "hep-th/9901001v1"
