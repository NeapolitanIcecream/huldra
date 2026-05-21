from __future__ import annotations

from pathlib import Path

import pytest

from huldra.config import HuldraSettings


def test_settings_default_to_local_safe_api_and_conservative_interval() -> None:
    settings = HuldraSettings()
    assert settings.api_host == "127.0.0.1"
    assert settings.api_port == 8765
    assert settings.request_interval_seconds >= 3.0
    assert str(settings.db_path).endswith("huldra.db")


def test_settings_read_huldra_environment_variables(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("HULDRA_DB_PATH", str(tmp_path / "custom.db"))
    monkeypatch.setenv("HULDRA_REQUEST_INTERVAL_SECONDS", "7")
    settings = HuldraSettings()
    assert settings.db_path == tmp_path / "custom.db"
    assert settings.request_interval_seconds == 7


def test_request_interval_rejects_values_below_arxiv_floor() -> None:
    with pytest.raises(ValueError, match="at least 3"):
        HuldraSettings(request_interval_seconds=2.9)
