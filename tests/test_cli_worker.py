from __future__ import annotations

import json

from typer.testing import CliRunner

from huldra.cli import app


def test_store_status_and_worker_once_cli(tmp_path) -> None:  # type: ignore[no-untyped-def]
    db = tmp_path / "huldra.db"
    runner = CliRunner()
    init = runner.invoke(app, ["store", "init", "--db", str(db)])
    status = runner.invoke(app, ["status", "--db", str(db), "--json"])
    worker = runner.invoke(app, ["worker", "--db", str(db), "--once", "--json"])
    assert init.exit_code == 0
    assert status.exit_code == 0
    assert worker.exit_code == 0
    assert json.loads(status.output)["queue_depth_total"] == 0
    assert json.loads(worker.output)["status"] == "idle"
