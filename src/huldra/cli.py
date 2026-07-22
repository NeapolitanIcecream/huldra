from __future__ import annotations

import time
from datetime import date, timedelta
from pathlib import Path
from typing import Annotated

import httpx
import orjson
import typer
import uvicorn

from huldra import __version__
from huldra.broker import HuldraBroker
from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.keys import normalize_arxiv_id
from huldra.models import ArxivRequest, CachePolicy, LegacySyncMode, OaiHarvestMode, OaiHarvestRequest
from huldra.planner import build_submitted_date_windows
from huldra.time import parse_datetime, utc_now
from huldra.worker import HuldraWorker, WorkerPassResult

app = typer.Typer(
    help="Huldra: local arXiv metadata broker.",
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
)
store_app = typer.Typer(help="Manage the Huldra SQLite store.", no_args_is_help=True)
harvest_app = typer.Typer(help="Run metadata harvest jobs.", no_args_is_help=True)
app.add_typer(store_app, name="store")
app.add_typer(harvest_app, name="harvest")

_IMMEDIATE_WORKER_STATUSES = frozenset({"cache_hit", "completed", "failed"})


def _print_json(payload: object) -> None:
    typer.echo(orjson.dumps(payload, option=orjson.OPT_INDENT_2).decode())


def _print_json_line(payload: object) -> None:
    typer.echo(orjson.dumps(payload).decode())


def _settings(db: Path | None = None) -> HuldraSettings:
    settings = HuldraSettings()
    if db is None:
        return settings
    return settings.model_copy(update={"db_path": db.expanduser()})


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("expected YYYY-MM-DD") from exc


def _parse_sync_mode(value: str) -> LegacySyncMode:
    normalized = value.strip().replace("-", "_")
    try:
        return LegacySyncMode(normalized)
    except ValueError as exc:
        raise typer.BadParameter("expected slice or complete-window") from exc


def _validate_sync_mode_wait(mode: LegacySyncMode, wait: bool) -> None:
    if mode == LegacySyncMode.COMPLETE_WINDOW and not wait:
        raise typer.BadParameter("--mode complete-window requires --wait")


def _parse_oai_mode(value: str) -> OaiHarvestMode:
    normalized = value.strip().replace("-", "_")
    try:
        return OaiHarvestMode(normalized)
    except ValueError as exc:
        raise typer.BadParameter("expected initial or incremental") from exc


def _worker_sleep_seconds(result: WorkerPassResult, settings: HuldraSettings) -> float:
    if result.status in _IMMEDIATE_WORKER_STATUSES:
        return 0.0
    if result.cooldown_until is not None:
        return max(0.0, (result.cooldown_until - utc_now()).total_seconds())
    return settings.worker_poll_interval_seconds


def _huldra_daemon_is_healthy(host: str, port: int) -> bool:
    probe_host = "127.0.0.1" if host == "0.0.0.0" else "::1" if host == "::" else host
    probe_url = httpx.URL(scheme="http", host=probe_host, port=port, path="/v1/status")
    try:
        with httpx.Client(timeout=1.0, trust_env=False) as client:
            response = client.get(probe_url)
            payload = response.json()
    except (httpx.RequestError, ValueError):
        return False
    required_fields = {"upstream_requests_total", "queue_depth_total", "cache_entries_total"}
    return response.status_code == 200 and isinstance(payload, dict) and required_fields <= payload.keys()


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


@store_app.command("gc")
def store_gc(
    db: Annotated[Path | None, typer.Option("--db", help="SQLite database path.")] = None,
    older_than_days: Annotated[
        int,
        typer.Option("--older-than-days", min=1, help="Delete records older than this many days."),
    ] = 30,
    apply_changes: Annotated[
        bool,
        typer.Option("--apply", help="Apply deletions; the default is a dry run."),
    ] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    settings = _settings(db)
    store = HuldraStore(settings.db_path)
    try:
        result = store.gc(
            cutoff=utc_now() - timedelta(days=older_than_days),
            dry_run=not apply_changes,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--db") from exc
    payload = result.model_dump(mode="json")
    if json_output:
        _print_json(payload)
    else:
        typer.echo(payload)


@store_app.command("vacuum")
def store_vacuum(
    db: Annotated[Path | None, typer.Option("--db", help="SQLite database path.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    """Rewrite an initialized store to reclaim free SQLite pages."""
    settings = _settings(db)
    try:
        result = HuldraStore(settings.db_path).vacuum()
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--db") from exc
    payload = result.model_dump(mode="json")
    if json_output:
        _print_json(payload)
    else:
        typer.echo(payload)


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
        typer.Option("--poll-interval-seconds", min=1.0, help="Idle sleep seconds (minimum 1)."),
    ] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
    emit_idle: Annotated[
        bool,
        typer.Option("--emit-idle", help="Print idle worker passes."),
    ] = False,
) -> None:
    settings = _settings(db)
    if poll_interval_seconds is not None:
        settings = settings.model_copy(update={"worker_poll_interval_seconds": poll_interval_seconds})
    store = HuldraStore(settings.db_path)
    worker_instance = HuldraWorker(store, settings)
    while True:
        result = worker_instance.run_once()
        payload = result.as_payload()
        if result.status != "idle" or emit_idle:
            if json_output:
                _print_json_line(payload)
            else:
                typer.echo(payload)
        if once:
            return
        sleep_seconds = _worker_sleep_seconds(result, settings)
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


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
    if _huldra_daemon_is_healthy(settings.api_host, settings.api_port):
        payload = {
            "status": "already_running",
            "host": settings.api_host,
            "port": settings.api_port,
        }
        if json_output:
            _print_json(payload)
        else:
            typer.echo(payload)
        return
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
def sync(
    db: Annotated[Path | None, typer.Option("--db", help="SQLite database path.")] = None,
    search_query: Annotated[
        list[str] | None,
        typer.Option("--search-query", help="arXiv search_query. Repeat for multiple queries."),
    ] = None,
    date_value: Annotated[
        str | None,
        typer.Option("--date", help="Submitted-date UTC day to sync."),
    ] = None,
    max_results: Annotated[int, typer.Option("--max-results", min=1, max=2000)] = 50,
    mode: Annotated[
        str,
        typer.Option("--mode", help="slice or complete-window."),
    ] = "slice",
    client_id: Annotated[str, typer.Option("--client-id", help="Client identifier.")] = "cli-sync",
    wait: Annotated[bool, typer.Option("--wait", help="Drain this request set inline.")] = False,
    wait_timeout_seconds: Annotated[
        float | None,
        typer.Option("--wait-timeout-seconds", help="Maintenance wait timeout."),
    ] = None,
    max_pages_per_window: Annotated[
        int,
        typer.Option("--max-pages-per-window", min=1, max=10_000),
    ] = 100,
    max_requests_total: Annotated[
        int,
        typer.Option("--max-requests-total", min=1, max=100_000),
    ] = 500,
    json_output: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    queries = search_query or []
    if date_value is None:
        raise typer.BadParameter("--date is required")
    parsed_date = _parse_date(date_value)
    requests = build_submitted_date_windows(
        search_queries=queries,
        start_date=parsed_date,
        end_date=parsed_date,
        max_results=max_results,
        client_id=client_id,
    )
    broker = HuldraBroker(settings=_settings(db))
    parsed_mode = _parse_sync_mode(mode)
    _validate_sync_mode_wait(parsed_mode, wait)
    payload = broker.sync_windows(
        requests,
        wait=wait,
        wait_timeout_seconds=wait_timeout_seconds,
        mode=parsed_mode,
        max_pages_per_window=max_pages_per_window,
        max_requests_total=max_requests_total,
    ).model_dump(mode="json")
    if json_output:
        _print_json(payload)
    else:
        typer.echo(payload)


@app.command()
def backfill(
    db: Annotated[Path | None, typer.Option("--db", help="SQLite database path.")] = None,
    search_query: Annotated[
        list[str] | None,
        typer.Option("--search-query", help="arXiv search_query. Repeat for multiple queries."),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="First UTC date, inclusive."),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="Last UTC date, inclusive."),
    ] = None,
    max_results: Annotated[int, typer.Option("--max-results", min=1, max=2000)] = 50,
    mode: Annotated[
        str,
        typer.Option("--mode", help="slice or complete-window."),
    ] = "slice",
    client_id: Annotated[str, typer.Option("--client-id", help="Client identifier.")] = "huldra-backfill",
    wait: Annotated[bool, typer.Option("--wait", help="Drain this request set inline.")] = False,
    wait_timeout_seconds: Annotated[
        float | None,
        typer.Option("--wait-timeout-seconds", help="Maintenance wait timeout."),
    ] = None,
    max_pages_per_window: Annotated[
        int,
        typer.Option("--max-pages-per-window", min=1, max=10_000),
    ] = 100,
    max_requests_total: Annotated[
        int,
        typer.Option("--max-requests-total", min=1, max=100_000),
    ] = 500,
    json_output: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    if start_date is None:
        raise typer.BadParameter("--start-date is required")
    parsed_start = _parse_date(start_date)
    resolved_end = _parse_date(end_date) if end_date is not None else parsed_start
    broker = HuldraBroker(settings=_settings(db))
    parsed_mode = _parse_sync_mode(mode)
    _validate_sync_mode_wait(parsed_mode, wait)
    payload = broker.backfill_windows(
        search_queries=search_query or [],
        start_date=parsed_start,
        end_date=resolved_end,
        max_results=max_results,
        wait=wait,
        wait_timeout_seconds=wait_timeout_seconds,
        mode=parsed_mode,
        client_id=client_id,
        max_pages_per_window=max_pages_per_window,
        max_requests_total=max_requests_total,
    ).model_dump(mode="json")
    if json_output:
        _print_json(payload)
    else:
        typer.echo(payload)


@harvest_app.command("oai")
def harvest_oai(
    db: Annotated[Path | None, typer.Option("--db", help="SQLite database path.")] = None,
    metadata_prefix: Annotated[
        str,
        typer.Option("--metadata-prefix", help="OAI metadata prefix: arXiv or arXivRaw."),
    ] = "arXiv",
    set_spec: Annotated[
        str | None,
        typer.Option("--set", help="Optional OAI setSpec, for example cs:cs:AI."),
    ] = None,
    from_datestamp: Annotated[
        str | None,
        typer.Option("--from", help="Optional OAI from datestamp."),
    ] = None,
    until_datestamp: Annotated[
        str | None,
        typer.Option("--until", help="Optional OAI until datestamp."),
    ] = None,
    resumption_token: Annotated[
        str | None,
        typer.Option(
            "--resumption-token",
            help="Optional OAI resumptionToken to continue a harvest.",
        ),
    ] = None,
    mode: Annotated[
        str,
        typer.Option("--mode", help="initial or incremental."),
    ] = "incremental",
    client_id: Annotated[str, typer.Option("--client-id", help="Client identifier.")] = "cli-harvest",
    max_pages: Annotated[int, typer.Option("--max-pages", min=1, max=100_000)] = 1000,
    max_requests: Annotated[int, typer.Option("--max-requests", min=1, max=100_000)] = 1000,
    runtime_budget_seconds: Annotated[
        float,
        typer.Option("--runtime-budget-seconds", min=0.001, max=604_800),
    ] = 3600.0,
    json_output: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    if metadata_prefix not in {"arXiv", "arXivRaw"}:
        raise typer.BadParameter("--metadata-prefix must be arXiv or arXivRaw")
    broker = HuldraBroker(settings=_settings(db))
    payload = broker.harvest_oai(
        OaiHarvestRequest(
            client_id=client_id,
            metadata_prefix=metadata_prefix,  # type: ignore[arg-type]
            set_spec=set_spec,
            from_datestamp=from_datestamp,
            until_datestamp=until_datestamp,
            resumption_token=resumption_token,
            mode=_parse_oai_mode(mode),
            max_pages=max_pages,
            max_requests=max_requests,
            runtime_budget_seconds=runtime_budget_seconds,
        )
    ).model_dump(mode="json")
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
