from __future__ import annotations

from pathlib import Path

from platformdirs import user_data_dir
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from huldra import __version__


def default_db_path() -> Path:
    return Path(user_data_dir("huldra")) / "huldra.db"


class HuldraSettings(BaseSettings):
    """Runtime settings for the local Huldra broker."""

    db_path: Path = Field(default_factory=default_db_path)
    api_host: str = "127.0.0.1"
    api_port: int = 8765
    request_interval_seconds: float = 5.0
    cooldown_seconds: int = 3600
    request_timeout_seconds: float = 30.0
    worker_poll_interval_seconds: float = 300.0
    lease_timeout_seconds: int = 120
    queue_claim_timeout_seconds: int = 300
    maturity_lag_days: int = 1
    user_agent: str = f"Huldra/{__version__} (local arxiv metadata broker; contact: unset)"
    arxiv_api_url: str = "https://export.arxiv.org/api/query"
    arxiv_oai_pmh_url: str = "https://oaipmh.arxiv.org/oai"
    legacy_search_window_result_cap: int = 10000
    oai_overlap_seconds: int = 0

    model_config = SettingsConfigDict(
        env_prefix="HULDRA_",
        extra="ignore",
        env_file=".env",
    )

    @field_validator("request_interval_seconds")
    @classmethod
    def _request_interval_respects_arxiv_floor(cls, value: float) -> float:
        if value < 3.0:
            raise ValueError("request_interval_seconds must be at least 3.0")
        return value

    @field_validator("api_port")
    @classmethod
    def _valid_port(cls, value: int) -> int:
        if value < 1 or value > 65535:
            raise ValueError("api_port must be between 1 and 65535")
        return value

    @field_validator("cooldown_seconds", "lease_timeout_seconds")
    @classmethod
    def _positive_integer(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("value must be positive")
        return value

    @field_validator("legacy_search_window_result_cap")
    @classmethod
    def _positive_window_cap(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("legacy_search_window_result_cap must be positive")
        return value

    @field_validator("oai_overlap_seconds")
    @classmethod
    def _non_negative_overlap(cls, value: int) -> int:
        if value < 0:
            raise ValueError("oai_overlap_seconds cannot be negative")
        return value
