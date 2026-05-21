from __future__ import annotations

import time
from pathlib import Path
from typing import Annotated

import orjson
import typer
import uvicorn

from huldra import __version__
from huldra.broker import HuldraBroker
from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.keys import normalize_arxiv_id
from huldra.models import ArxivRequest, CachePolicy
from huldra.time import parse_datetime
from huldra.worker import HuldraWorker

app = typer.Typer(
    help="Huldra: local arXiv metadata broker.",
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
)
store_app = typer.Typer(help="Manage the Huldra SQLite store.", no_args_is_help=True)
app.add_typer(store_app, name="store")


def _print_json(payload: object) -> None:
    typer.echo(orjson.dumps(payload, option=orjson.OPT_INDENT_2).decode())


def _settings(db: Path | None = None) -> HuldraSettings:
    settings = HuldraSettings()
    if db is None:
        return settings
    return settings.model_copy(update={"db_path": db.expanduser()})


@app.command()
def version() -> None:
    typer.echo(f"huldra {__version__}")


@store_app.command("init")
def store_init(
    db: Annotated[Path | None, typer.Option("--db", help="SQLite database path.")] = None,
) -> None:
    settings = _settings(db)
    HuldraStore(settings.db_path).init_schema()
    typer.echo(str(settings.db_path))


@app.command()
def status(
    db: Annotated[Path | None, typer.Option("--db", help="SQLite database path.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    settings = _settings(db)
    broker = HuldraBroker(settings=settings)
    payload = broker.status().model_dump(mode="json")
    if json_output:
        _print_json(payload)
    else:
        typer.echo(payload)


@app.command()
def worker(
    db: Annotated[Path | None, typer.Option("--db", help="SQLite database path.")] = None,
    once: Annotated[bool, typer.Option("--once", help="Run one worker pass.")] = False,
    poll_interval_seconds: Annotated[
        float | None,
        typer.Option("--poll-interval-seconds", help="Idle sleep seconds."),
    ] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    settings = _settings(db)
    if poll_interval_seconds is not None:
        settings = settings.model_copy(update={"worker_poll_interval_seconds": poll_interval_seconds})
    store = HuldraStore(settings.db_path)
    worker_instance = HuldraWorker(store, settings)
    while True:
        result = worker_instance.run_once()
        payload = result.as_payload()
        if json_output:
            _print_json(payload)
        else:
            typer.echo(payload)
        if once:
            return
        time.sleep(settings.worker_poll_interval_seconds)


@app.command()
def daemon(
    db: Annotated[Path | None, typer.Option("--db", help="SQLite database path.")] = None,
    host: Annotated[str | None, typer.Option("--host", help="Bind host.")] = None,
    port: Annotated[int | None, typer.Option("--port", help="Bind port.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit startup JSON.")] = False,
) -> None:
    settings = _settings(db)
    if host is not None:
        settings = settings.model_copy(update={"api_host": host})
    if port is not None:
        settings = settings.model_copy(update={"api_port": port})
    if settings.api_host == "0.0.0.0":
        typer.echo(
            "warning: Huldra has no built-in auth; avoid exposing it publicly.",
            err=True,
        )
    if json_output:
        _print_json(
            {
                "status": "starting",
                "db_path": str(settings.db_path),
                "host": settings.api_host,
                "port": settings.api_port,
            }
        )
    from huldra.api import create_app

    uvicorn.run(create_app(settings), host=settings.api_host, port=settings.api_port)


@app.command()
def query(
    db: Annotated[Path | None, typer.Option("--db", help="SQLite database path.")] = None,
    client_id: Annotated[str, typer.Option("--client-id", help="Client identifier.")] = "cli",
    search_query: Annotated[
        str | None,
        typer.Option("--search-query", help="arXiv search_query."),
    ] = None,
    id_list: Annotated[
        str | None,
        typer.Option("--id-list", help="Comma-separated arXiv IDs."),
    ] = None,
    max_results: Annotated[int, typer.Option("--max-results", min=1, max=2000)] = 50,
    start: Annotated[int, typer.Option("--start", min=0)] = 0,
    sort_by: Annotated[str, typer.Option("--sort-by")] = "submittedDate",
    sort_order: Annotated[str, typer.Option("--sort-order")] = "descending",
    wait: Annotated[bool, typer.Option("--wait", help="Wait for completed cache.")] = False,
    timeout_seconds: Annotated[
        float | None,
        typer.Option("--timeout-seconds", help="Wait timeout."),
    ] = None,
    submitted_start: Annotated[
        str | None,
        typer.Option("--submitted-start", help="UTC ISO datetime."),
    ] = None,
    submitted_end: Annotated[
        str | None,
        typer.Option("--submitted-end", help="UTC ISO datetime."),
    ] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    settings = _settings(db)
    broker = HuldraBroker(settings=settings)
    ids = tuple(normalize_arxiv_id(part) for part in (id_list or "").split(",") if part.strip())
    request = ArxivRequest(
        client_id=client_id,
        search_query=search_query,
        id_list=ids,
        max_results=max_results,
        start=start,
        sort_by=sort_by,  # type: ignore[arg-type]
        sort_order=sort_order,  # type: ignore[arg-type]
        submitted_start=parse_datetime(submitted_start) if submitted_start else None,
        submitted_end=parse_datetime(submitted_end) if submitted_end else None,
        cache_policy=CachePolicy.WAIT_UNTIL_READY if wait else CachePolicy.CACHE_OR_ENQUEUE,
        timeout_seconds=timeout_seconds,
    )
    payload = broker.ensure(request).model_dump(mode="json")
    if json_output:
        _print_json(payload)
    else:
        typer.echo(payload)


@app.command()
def result(
    cache_key: Annotated[str, typer.Option("--cache-key", help="Cache key.")],
    db: Annotated[Path | None, typer.Option("--db", help="SQLite database path.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    broker = HuldraBroker(settings=_settings(db))
    payload = broker.get_result(cache_key).model_dump(mode="json")
    if json_output:
        _print_json(payload)
    else:
        typer.echo(payload)


@app.command()
def paper(
    arxiv_id: Annotated[str, typer.Option("--arxiv-id", help="arXiv ID.")],
    db: Annotated[Path | None, typer.Option("--db", help="SQLite database path.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    settings = _settings(db)
    store = HuldraStore(settings.db_path)
    store.init_schema()
    value = store.get_paper(normalize_arxiv_id(arxiv_id))
    payload = value.model_dump(mode="json") if value is not None else None
    if json_output:
        _print_json(payload)
    else:
        typer.echo(payload)
