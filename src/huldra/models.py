from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from huldra.time import ensure_utc


class CachePolicy(StrEnum):
    CACHE_ONLY = "cache_only"
    CACHE_OR_ENQUEUE = "cache_or_enqueue"
    WAIT_UNTIL_READY = "wait_until_ready"
    STALE_WHILE_REVALIDATE = "stale_while_revalidate"


class ReadinessMode(StrEnum):
    RAW_COMPLETED = "raw_completed"
    ANALYSIS_READY = "analysis_ready"


class RequestStatus(StrEnum):
    QUEUED = "queued"
    DELAYED = "delayed"
    CLAIMED = "claimed"
    COMPLETED = "completed"
    FAILED = "failed"


SortBy = Literal["relevance", "lastUpdatedDate", "submittedDate"]
SortOrder = Literal["ascending", "descending"]
ApiFamily = Literal["legacy_search"]


class HuldraModel(BaseModel):
    model_config = ConfigDict(use_enum_values=True, extra="forbid")


class ArxivRequest(HuldraModel):
    client_id: str
    search_query: str | None = None
    id_list: tuple[str, ...] = ()
    sort_by: SortBy = "submittedDate"
    sort_order: SortOrder = "descending"
    start: int = Field(default=0, ge=0)
    max_results: int = Field(default=50, ge=1, le=2000)
    submitted_start: datetime | None = None
    submitted_end: datetime | None = None
    cache_policy: CachePolicy = CachePolicy.CACHE_OR_ENQUEUE
    readiness: ReadinessMode = ReadinessMode.RAW_COMPLETED
    priority: int = 0
    timeout_seconds: float | None = Field(default=None, gt=0)
    api_family: ApiFamily = "legacy_search"

    @field_validator("client_id")
    @classmethod
    def _client_id_not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("client_id cannot be blank")
        return value

    @field_validator("submitted_start", "submitted_end")
    @classmethod
    def _datetime_to_utc(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return ensure_utc(value)

    @model_validator(mode="after")
    def _validate_request_shape(self) -> ArxivRequest:
        query = self.search_query.strip() if self.search_query else None
        ids = tuple(item.strip() for item in self.id_list if item.strip())
        if not query and not ids:
            raise ValueError("search_query or id_list is required")
        object.__setattr__(self, "search_query", query)
        object.__setattr__(self, "id_list", ids)
        if (self.submitted_start is None) != (self.submitted_end is None):
            raise ValueError("submitted_start and submitted_end must be provided together")
        if self.submitted_start is not None and self.submitted_end is not None:
            if self.submitted_start >= self.submitted_end:
                raise ValueError("submitted_start must be before submitted_end")
            if query and "submitteddate:" in query.lower():
                raise ValueError(
                    "do not combine submittedDate in search_query with submitted_start/submitted_end"
                )
        return self


class ArxivPaper(HuldraModel):
    arxiv_id: str
    version: int | None = None
    canonical_url: str
    title: str
    abstract: str | None = None
    authors: list[str] = Field(default_factory=list)
    primary_category: str | None = None
    categories: list[str] = Field(default_factory=list)
    published_at: datetime | None = None
    updated_at: datetime | None = None
    comment: str | None = None
    journal_ref: str | None = None
    doi: str | None = None
    raw_atom: dict[str, Any] = Field(default_factory=dict)

    @field_validator("published_at", "updated_at")
    @classmethod
    def _paper_datetime_to_utc(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return ensure_utc(value)


class RateState(HuldraModel):
    name: str = "arxiv_legacy_api"
    last_request_at: datetime | None = None
    cooldown_until: datetime | None = None
    consecutive_429_total: int = 0
    last_status: int | None = None
    last_error_message: str | None = None


class QueueItem(HuldraModel):
    request_id: str
    cache_key: str
    client_id: str
    request: ArxivRequest
    priority: int
    status: RequestStatus
    created_at: datetime
    updated_at: datetime
    claimed_by: str | None = None
    claimed_until: datetime | None = None
    attempts_total: int = 0
    next_attempt_at: datetime | None = None
    completed_at: datetime | None = None
    error_category: str | None = None
    error_message: str | None = None


class CacheEntry(HuldraModel):
    cache_key: str
    request: ArxivRequest
    api_family: str
    status: str
    requested_at: datetime | None = None
    completed_at: datetime | None = None
    cooldown_until: datetime | None = None
    upstream_status: int | None = None
    upstream_requests_total: int = 0
    result_count: int = 0
    total_results: int | None = None
    error_category: str | None = None
    error_message: str | None = None


class BrokerStatus(HuldraModel):
    upstream_requests_total: int = 0
    upstream_429_total: int = 0
    cooldown_until: datetime | None = None
    cooldown_active: bool = False
    queue_depth_total: int = 0
    queue_ready_total: int = 0
    queue_delayed_total: int = 0
    cache_entries_total: int = 0
    cache_completed_total: int = 0
    cache_failed_total: int = 0
    papers_total: int = 0
    worker_last_heartbeat_at: datetime | None = None
    worker_next_wake_at: datetime | None = None
    oldest_pending_request_at: datetime | None = None


class ArxivResult(HuldraModel):
    status: str
    cache_key: str
    request_id: str | None = None
    papers: list[ArxivPaper] = Field(default_factory=list)
    papers_total: int = 0
    total_results: int | None = None
    cache_hit: bool = False
    stale: bool = False
    ready: bool = False
    analysis_ready: bool = False
    maturity_applicable: bool = False
    maturity_cutoff: datetime | None = None
    cooldown_until: datetime | None = None
    blocked_reason: str | None = None
    error_category: str | None = None
    error_message: str | None = None
    completed_at: datetime | None = None
    queued_at: datetime | None = None
    upstream_status: int | None = None


def utc_day_floor(value: datetime) -> datetime:
    value = ensure_utc(value)
    return datetime(value.year, value.month, value.day, tzinfo=UTC)
