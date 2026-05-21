from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.models import ArxivPaper


@pytest.fixture
def settings(tmp_path: Path) -> HuldraSettings:
    return HuldraSettings(
        db_path=tmp_path / "huldra.db",
        request_interval_seconds=3.0,
        cooldown_seconds=60,
        worker_poll_interval_seconds=1.0,
        request_timeout_seconds=0.2,
    )


@pytest.fixture
def store(settings: HuldraSettings) -> HuldraStore:
    value = HuldraStore(settings.db_path)
    value.init_schema()
    return value


def make_paper(arxiv_id: str = "2401.00001v1") -> ArxivPaper:
    return ArxivPaper(
        arxiv_id=arxiv_id,
        version=1,
        canonical_url=f"https://arxiv.org/abs/{arxiv_id}",
        title="Test Paper",
        abstract="An abstract.",
        authors=["Ada Lovelace"],
        primary_category="cs.AI",
        categories=["cs.AI"],
        published_at=datetime(2024, 1, 1, tzinfo=UTC),
        updated_at=datetime(2024, 1, 2, tzinfo=UTC),
        raw_atom={"entry_id": f"https://arxiv.org/abs/{arxiv_id}"},
    )
