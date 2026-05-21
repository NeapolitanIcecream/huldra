from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Query

from huldra.broker import HuldraBroker
from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.keys import normalize_arxiv_id
from huldra.models import (
    ArxivPaper,
    ArxivRequest,
    ArxivResult,
    BrokerStatus,
    CachePolicy,
)


def create_app(settings: HuldraSettings | None = None) -> FastAPI:
    resolved = settings or HuldraSettings()
    app = FastAPI(title="Huldra", version="0.1.0")
    store = HuldraStore(resolved.db_path)
    broker = HuldraBroker(store=store, settings=resolved)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/v1/status", response_model=BrokerStatus)
    def status() -> BrokerStatus:
        return broker.status()

    @app.post("/v1/requests", response_model=ArxivResult)
    def create_request(
        request: ArxivRequest,
        wait: bool = Query(default=False),
    ) -> ArxivResult:
        if wait:
            request = request.model_copy(update={"cache_policy": CachePolicy.WAIT_UNTIL_READY})
        return broker.ensure(request)

    @app.get("/v1/requests/{request_id}")
    def get_request(request_id: str) -> dict[str, object]:
        item = store.get_queue_item(request_id)
        if item is None:
            raise HTTPException(status_code=404, detail="request not found")
        return item.model_dump(mode="json")

    @app.get("/v1/results/{cache_key}", response_model=ArxivResult)
    def get_result(cache_key: str) -> ArxivResult:
        return broker.get_result(cache_key)

    @app.get("/v1/papers/{arxiv_id:path}", response_model=ArxivPaper | None)
    def get_paper(arxiv_id: str) -> ArxivPaper | None:
        paper = store.get_paper(normalize_arxiv_id(arxiv_id))
        if paper is None:
            raise HTTPException(status_code=404, detail="paper not found")
        return paper

    return app


def settings_with_db(db_path: Path) -> HuldraSettings:
    return HuldraSettings(db_path=db_path)
