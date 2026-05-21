from __future__ import annotations

from huldra.db import HuldraStore
from huldra.models import ArxivRequest, QueueItem


class HuldraQueue:
    def __init__(self, store: HuldraStore) -> None:
        self.store = store

    def enqueue(self, request: ArxivRequest, cache_key: str | None = None) -> QueueItem:
        return self.store.enqueue_request(request, cache_key)

    def claim_next(
        self,
        *,
        owner_token: str,
        claim_timeout_seconds: int = 300,
        cache_keys: set[str] | frozenset[str] | None = None,
    ) -> QueueItem | None:
        return self.store.claim_next_queue_item(
            owner_token=owner_token,
            claim_timeout_seconds=claim_timeout_seconds,
            cache_keys=cache_keys,
        )
