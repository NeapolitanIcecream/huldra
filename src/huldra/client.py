from __future__ import annotations

from types import TracebackType
from typing import Any
from urllib.parse import quote

import httpx

from huldra.models import ArxivPaper, ArxivRequest, ArxivResult, BrokerStatus


class HuldraClientError(RuntimeError):
    pass


class HuldraHTTPError(HuldraClientError):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


class HuldraClient:
    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8765",
        timeout: float = 30.0,
        *,
        client: httpx.Client | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._client = client or httpx.Client(base_url=self.base_url, timeout=timeout)
        self._owns_client = client is None

    def __enter__(self) -> HuldraClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def status(self) -> BrokerStatus:
        return BrokerStatus.model_validate(self._json(self._client.get("/v1/status")))

    def ensure(self, request: ArxivRequest, *, wait: bool = False) -> ArxivResult:
        response = self._client.post(
            "/v1/requests",
            params={"wait": str(wait).lower()},
            json=request.model_dump(mode="json"),
        )
        return ArxivResult.model_validate(self._json(response))

    def ensure_search(
        self,
        *,
        search_query: str,
        max_results: int = 50,
        wait: bool = False,
        client_id: str = "python-client",
        **kwargs: Any,
    ) -> ArxivResult:
        return self.ensure(
            ArxivRequest(
                client_id=client_id,
                search_query=search_query,
                max_results=max_results,
                **kwargs,
            ),
            wait=wait,
        )

    def ensure_ids(
        self,
        ids: list[str],
        *,
        wait: bool = False,
        client_id: str = "python-client",
        **kwargs: Any,
    ) -> ArxivResult:
        return self.ensure(
            ArxivRequest(client_id=client_id, id_list=tuple(ids), **kwargs),
            wait=wait,
        )

    def get_result(self, cache_key: str) -> ArxivResult:
        return ArxivResult.model_validate(self._json(self._client.get(f"/v1/results/{cache_key}")))

    def get_paper(self, arxiv_id: str) -> ArxivPaper | None:
        encoded = quote(arxiv_id, safe="")
        response = self._client.get(f"/v1/papers/{encoded}")
        if response.status_code == 404:
            return None
        return ArxivPaper.model_validate(self._json(response))

    def _json(self, response: httpx.Response) -> Any:
        if response.status_code >= 400:
            raise HuldraHTTPError(response.status_code, response.text)
        return response.json()
