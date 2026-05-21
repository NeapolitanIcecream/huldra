from __future__ import annotations

from huldra.db import HuldraStore
from huldra.models import BrokerStatus


def collect_status(store: HuldraStore) -> BrokerStatus:
    return store.status_summary()
