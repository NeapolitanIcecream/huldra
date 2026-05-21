from __future__ import annotations

from huldra.db import HuldraStore
from huldra.models import ArxivRequest
from huldra.queue import HuldraQueue


def test_queue_orders_higher_priority_first(store: HuldraStore) -> None:
    queue = HuldraQueue(store)
    queue.enqueue(ArxivRequest(client_id="low", search_query="cat:cs.AI", priority=0))
    high = queue.enqueue(ArxivRequest(client_id="high", search_query="cat:cs.LG", priority=10))
    claimed = queue.claim_next(owner_token="worker")
    assert claimed is not None
    assert claimed.request_id == high.request_id
