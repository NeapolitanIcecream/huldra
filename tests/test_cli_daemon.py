from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
from pytest import MonkeyPatch
from typer.testing import CliRunner

import huldra.cli as cli


def test_daemon_exits_successfully_when_healthy_huldra_owns_endpoint(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(cli, "_huldra_daemon_is_healthy", lambda _host, _port: True)

    def unexpected_uvicorn_run(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("a healthy existing daemon must not start another server")

    monkeypatch.setattr(cli.uvicorn, "run", unexpected_uvicorn_run)

    result = CliRunner().invoke(
        cli.app,
        [
            "daemon",
            "--db",
            str(tmp_path / "unused.db"),
            "--host",
            "127.0.0.1",
            "--port",
            "8765",
            "--json",
        ],
    )

    assert result.exit_code == 0
    assert json.loads(result.output)["status"] == "already_running"
    assert not (tmp_path / "unused.db").exists()


def test_daemon_starts_when_no_healthy_huldra_owns_endpoint(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(cli, "_huldra_daemon_is_healthy", lambda _host, _port: False)
    started: list[tuple[str, int]] = []

    def capture_uvicorn_run(_app: object, *, host: str, port: int) -> None:
        started.append((host, port))

    monkeypatch.setattr(cli.uvicorn, "run", capture_uvicorn_run)

    result = CliRunner().invoke(
        cli.app,
        [
            "daemon",
            "--db",
            str(tmp_path / "huldra.db"),
            "--host",
            "127.0.0.1",
            "--port",
            "9876",
        ],
    )

    assert result.exit_code == 0
    assert started == [("127.0.0.1", 9876)]


def test_daemon_brackets_ipv6_literal_when_probing_endpoint(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    requested_urls: list[str] = []

    def handle_probe(request: httpx.Request) -> httpx.Response:
        requested_urls.append(str(request.url))
        return httpx.Response(503, json={"status": "not_huldra"})

    transport = httpx.MockTransport(handle_probe)
    httpx_client = httpx.Client

    def client_with_mock_transport(**kwargs: Any) -> httpx.Client:
        return httpx_client(transport=transport, **kwargs)

    started: list[tuple[str, int]] = []

    def capture_uvicorn_run(_app: object, *, host: str, port: int) -> None:
        started.append((host, port))

    monkeypatch.setattr(cli.httpx, "Client", client_with_mock_transport)
    monkeypatch.setattr(cli.uvicorn, "run", capture_uvicorn_run)

    result = CliRunner().invoke(
        cli.app,
        [
            "daemon",
            "--db",
            str(tmp_path / "huldra.db"),
            "--host",
            "::1",
            "--port",
            "9876",
        ],
    )

    assert result.exit_code == 0
    assert requested_urls == ["http://[::1]:9876/v1/status"]
    assert started == [("::1", 9876)]
