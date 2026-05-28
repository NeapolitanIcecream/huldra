from __future__ import annotations

from datetime import UTC, date, datetime
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


class QueueWorkKind(StrEnum):
    FETCH_MISSING = "fetch_missing"
    REFRESH_COMPLETED = "refresh_completed"


class CoverageStatus(StrEnum):
    SLICE = "slice"
    COMPLETE = "complete"
    PARTIAL = "partial"
    OVERFLOW = "overflow"
    UNKNOWN = "unknown"


class LegacySyncMode(StrEnum):
    SLICE = "slice"
    COMPLETE_WINDOW = "complete_window"


class OaiHarvestMode(StrEnum):
    INITIAL = "initial"
    INCREMENTAL = "incremental"


SortBy = Literal["relevance", "lastUpdatedDate", "submittedDate"]
SortOrder = Literal["ascending", "descending"]
ApiFamily = Literal["legacy_search", "oai_pmh"]
OaiMetadataPrefix = Literal["arXiv", "arXivRaw"]


class HuldraModel(BaseModel):
    model_config = ConfigDict(use_enum_values=True, extra="forbid")


class HuldraResponseModel(BaseModel):
    model_config = ConfigDict(use_enum_values=True, extra="allow")


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
    maturity_lag_days: int | None = None
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

    @field_validator("api_family")
    @classmethod
    def _api_family_must_be_legacy_search(cls, value: ApiFamily) -> ApiFamily:
        if value != "legacy_search":
            raise ValueError("ArxivRequest only supports api_family='legacy_search'; use OaiHarvestRequest")
        return value

    @field_validator("submitted_start", "submitted_end")
    @classmethod
    def _datetime_to_utc(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        normalized = ensure_utc(value)
        if normalized.second != 0 or normalized.microsecond != 0:
            raise ValueError("submitted-date bounds must use UTC minute precision")
        return normalized

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
        if (
            ids
            and not query
            and self.submitted_start is None
            and self.submitted_end is None
            and self.start == 0
            and self.sort_by == "submittedDate"
            and self.sort_order == "descending"
            and self.max_results < len(ids)
        ):
            raise ValueError("max_results must be at least the number of requested IDs")
        return self


class ArxivPaper(HuldraResponseModel):
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
    authors_detail: list[dict[str, Any]] = Field(default_factory=list)
    license: str | None = None
    oai_identifier: str | None = None
    oai_datestamp: datetime | None = None
    oai_set_specs: list[str] = Field(default_factory=list)
    links: list[dict[str, Any]] = Field(default_factory=list)
    versions: list[dict[str, Any]] = Field(default_factory=list)
    withdrawn: bool = False
    deleted: bool = False
    raw_metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("published_at", "updated_at", "oai_datestamp")
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
    upstream_429_total: int = 0
    last_status: int | None = None
    last_error_message: str | None = None


class QueueItem(HuldraModel):
    request_id: str
    cache_key: str
    client_id: str
    request: ArxivRequest
    priority: int
    status: RequestStatus
    work_kind: QueueWorkKind = QueueWorkKind.FETCH_MISSING
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
    coverage_status: CoverageStatus = CoverageStatus.UNKNOWN
    error_category: str | None = None
    error_message: str | None = None


class BrokerStatus(HuldraResponseModel):
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
    worker_last_error_category: str | None = None
    worker_last_error_message: str | None = None
    oldest_pending_request_at: datetime | None = None


class ArxivResult(HuldraResponseModel):
    serving_mode: str = ReadinessMode.RAW_COMPLETED
    status: str
    cache_key: str
    request_id: str | None = None
    papers: list[ArxivPaper] = Field(default_factory=list)
    papers_total: int = 0
    cached_papers_total: int = 0
    total_results: int | None = None
    coverage_status: CoverageStatus = CoverageStatus.UNKNOWN
    cache_hit: bool = False
    stale: bool = False
    cache_readable: bool = False
    mature: bool = True
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


class ArxivRawInspectionResult(HuldraResponseModel):
    serving_mode: Literal["raw_inspection"] = "raw_inspection"
    status: str
    cache_key: str
    papers: list[ArxivPaper] = Field(default_factory=list)
    papers_total: int = 0
    total_results: int | None = None
    coverage_status: CoverageStatus = CoverageStatus.UNKNOWN
    cache_hit: bool = False
    cache_readable: bool = False
    cooldown_until: datetime | None = None
    blocked_reason: str | None = None
    error_category: str | None = None
    error_message: str | None = None
    completed_at: datetime | None = None
    upstream_status: int | None = None


class HuldraMaintenanceRequestResult(HuldraResponseModel):
    sync_job_id: str | None = None
    cache_key: str
    request_id: str | None = None
    search_query: str | None = None
    submitted_start: datetime | None = None
    submitted_end: datetime | None = None
    raw_cache_status: str
    serving_status: str | None = None
    coverage_status: CoverageStatus = CoverageStatus.UNKNOWN
    cache_hit: bool = False
    joined_existing_queue: bool = False
    upstream_status: int | None = None
    cooldown_until: datetime | None = None
    error_category: str | None = None
    error_message: str | None = None
    papers_total: int = 0
    result_count: int = 0
    total_results: int | None = None
    pages_total: int = 0
    pages_completed_total: int = 0


class HuldraMaintenanceResult(HuldraResponseModel):
    requested_total: int = 0
    queued_total: int = 0
    cache_miss_total: int = 0
    cache_hit_total: int = 0
    completed_windows_total: int = 0
    completed_slices_total: int = 0
    complete_windows_total: int = 0
    partial_windows_total: int = 0
    overflow_windows_total: int = 0
    upstream_requests_total: int = 0
    upstream_429_total: int = 0
    retry_after_seconds: int | None = None
    cooldown_active_total: int = 0
    skipped_windows_total: int = 0
    rate_limited_windows_total: int = 0
    failed_windows_total: int = 0
    papers_total: int = 0
    cooldown_active: bool = False
    cooldown_until: datetime | None = None
    requests: list[HuldraMaintenanceRequestResult] = Field(default_factory=list)


class HuldraSyncRequest(HuldraModel):
    requests: list[ArxivRequest]
    wait: bool = False
    wait_timeout_seconds: float | None = Field(default=None, gt=0)
    mode: LegacySyncMode = LegacySyncMode.SLICE

    @model_validator(mode="after")
    def _complete_window_requires_wait(self) -> HuldraSyncRequest:
        if self.mode == LegacySyncMode.COMPLETE_WINDOW and not self.wait:
            raise ValueError("complete_window mode requires wait=True")
        return self


class HuldraBackfillRequest(HuldraModel):
    search_queries: list[str]
    start_date: date
    end_date: date
    max_results: int = Field(default=50, ge=1, le=2000)
    wait: bool = False
    wait_timeout_seconds: float | None = Field(default=None, gt=0)
    mode: LegacySyncMode = LegacySyncMode.SLICE
    client_id: str = "huldra-backfill"

    @field_validator("search_queries")
    @classmethod
    def _search_queries_not_blank(cls, value: list[str]) -> list[str]:
        normalized = [item.strip() for item in value if item.strip()]
        if not normalized:
            raise ValueError("search_queries cannot be empty")
        return normalized

    @field_validator("client_id")
    @classmethod
    def _backfill_client_id_not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("client_id cannot be blank")
        return value

    @model_validator(mode="after")
    def _validate_backfill_dates(self) -> HuldraBackfillRequest:
        if self.start_date > self.end_date:
            raise ValueError("start_date must be on or before end_date")
        if self.mode == LegacySyncMode.COMPLETE_WINDOW and not self.wait:
            raise ValueError("complete_window mode requires wait=True")
        return self


class OaiHarvestRequest(HuldraModel):
    client_id: str = "huldra-oai"
    metadata_prefix: OaiMetadataPrefix = "arXiv"
    set_spec: str | None = None
    from_datestamp: str | None = None
    until_datestamp: str | None = None
    mode: OaiHarvestMode = OaiHarvestMode.INCREMENTAL
    cache_policy: CachePolicy = CachePolicy.CACHE_OR_ENQUEUE
    priority: int = 0
    timeout_seconds: float | None = Field(default=None, gt=0)

    @field_validator("client_id")
    @classmethod
    def _oai_client_id_not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("client_id cannot be blank")
        return value

    @field_validator("set_spec", "from_datestamp", "until_datestamp")
    @classmethod
    def _oai_optional_string_not_blank(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        return value or None


class OaiRecord(HuldraModel):
    oai_identifier: str
    arxiv_id: str | None = None
    metadata_prefix: OaiMetadataPrefix
    datestamp: datetime | None = None
    set_specs: list[str] = Field(default_factory=list)
    deleted: bool = False
    paper: ArxivPaper | None = None
    raw_xml: str | None = None

    @field_validator("datestamp")
    @classmethod
    def _datestamp_to_utc(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return ensure_utc(value)


class OaiHarvestResult(HuldraResponseModel):
    harvest_id: str
    status: str
    metadata_prefix: OaiMetadataPrefix
    set_spec: str | None = None
    mode: OaiHarvestMode
    records_processed: int = 0
    papers_upserted: int = 0
    deleted_records: int = 0
    pages_total: int = 0
    current_watermark: str | None = None
    resumption_token: str | None = None
    error_category: str | None = None
    error_message: str | None = None


def utc_day_floor(value: datetime) -> datetime:
    value = ensure_utc(value)
    return datetime(value.year, value.month, value.day, tzinfo=UTC)
