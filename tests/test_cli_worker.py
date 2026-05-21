from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import typer
from pytest import MonkeyPatch
from typer.testing import CliRunner

import huldra.cli as cli
from huldra.worker import WorkerPassResult


def test_store_status_and_worker_once_cli(tmp_path) -> None:  # type: ignore[no-untyped-def]
    db = tmp_path / "huldra.db"
    runner = CliRunner()
    init = runner.invoke(cli.app, ["store", "init", "--db", str(db)])
    status = runner.invoke(cli.app, ["status", "--db", str(db), "--json"])
    worker = runner.invoke(cli.app, ["worker", "--db", str(db), "--once", "--json"])
    assert init.exit_code == 0
    assert status.exit_code == 0
    assert worker.exit_code == 0
    assert json.loads(status.output)["queue_depth_total"] == 0
    assert json.loads(worker.output)["status"] == "idle"


def test_worker_cli_does_not_sleep_between_successful_passes(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Regression: successful worker passes slept for the idle poll interval."""

    class FakeWorker:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            self._results: Iterator[WorkerPassResult] = iter(
                (
                    WorkerPassResult(status="completed", request_id="r1", cache_key="k1"),
                    WorkerPassResult(status="cache_hit", request_id="r2", cache_key="k2"),
                    WorkerPassResult(status="idle"),
                )
            )

        def run_once(self) -> WorkerPassResult:
            return next(self._results)

    sleep_calls: list[float] = []

    def stop_after_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        raise typer.Exit(code=0)

    monkeypatch.setattr(cli, "HuldraWorker", FakeWorker)
    monkeypatch.setattr(cli.time, "sleep", stop_after_sleep)

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "worker",
            "--db",
            str(tmp_path / "huldra.db"),
            "--poll-interval-seconds",
            "7",
        ],
    )

    assert result.exit_code == 0
    assert result.output.count("'status': 'completed'") == 1
    assert result.output.count("'status': 'cache_hit'") == 1
    assert result.output.count("'status': 'idle'") == 1
    assert sleep_calls == [7.0]


def test_sync_and_backfill_cli_emit_json_summaries(tmp_path: Path) -> None:
    db = tmp_path / "huldra.db"
    runner = CliRunner()

    sync = runner.invoke(
        cli.app,
        [
            "sync",
            "--db",
            str(db),
            "--search-query",
            "cat:cs.AI",
            "--date",
            "2026-01-01",
            "--max-results",
            "10",
            "--json",
        ],
    )
    backfill = runner.invoke(
        cli.app,
        [
            "backfill",
            "--db",
            str(db),
            "--search-query",
            "cat:cs.LG",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-02",
            "--max-results",
            "10",
            "--json",
        ],
    )

    assert sync.exit_code == 0
    assert json.loads(sync.output)["requested_total"] == 1
    assert backfill.exit_code == 0
    assert json.loads(backfill.output)["requested_total"] == 2
