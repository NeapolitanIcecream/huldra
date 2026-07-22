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


def test_joined_request_raises_shared_queue_priority(store: HuldraStore) -> None:
    queue = HuldraQueue(store)
    shared = queue.enqueue(
        ArxivRequest(client_id="first", search_query="cat:cs.AI", priority=0)
    )
    queue.enqueue(ArxivRequest(client_id="other", search_query="cat:cs.LG", priority=5))
    joined = queue.enqueue(
        ArxivRequest(client_id="urgent", search_query="cat:cs.AI", priority=10)
    )

    claimed = queue.claim_next(owner_token="worker")

    assert joined.request_id == shared.request_id
    assert claimed is not None
    assert claimed.request_id == shared.request_id
    assert claimed.priority == 10
