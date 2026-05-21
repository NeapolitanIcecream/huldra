# Recoleta-Compatible arXiv Broker Proposal

Status: Proposed

Date: 2026-05-21

Related:

- `docs/design/local-arxiv-metadata-broker.md`
- Recoleta `docs/design/arxiv-paper-pool.md`
- Recoleta `docs/design/arxiv-pool-maturity-gate.md`
- Recoleta `docs/design/huldra-arxiv-pool-adapter-proposal.md`

## Summary

Make Huldra the reusable local arXiv metadata infrastructure that Recoleta and
other same-machine tools can share without duplicating arXiv API requests or
amplifying HTTP 429 events.

Huldra should own the generic broker responsibilities:

- upstream arXiv API fetches;
- durable single-machine rate limiting;
- HTTP 429 cooldown;
- queueing and request deduplication;
- SQLite metadata cache;
- cache refresh and backfill orchestration;
- generic cache/maturity primitives for submitted-date windows;
- a conservative `analysis_ready` serving mode that does not expose immature
  papers.

Recoleta should not depend on Huldra's SQLite schema or internal worker classes.
It should consume Huldra through the public Python client or local HTTP API and
translate Huldra metadata results into Recoleta `ItemDraft` objects in a thin
adapter.

## Goals

- Preserve arXiv legacy API politeness: one upstream connection and at least one
  request interval across all local consumers.
- Provide a stable public contract that can serve Recoleta's current arXiv pool
  semantics without importing Recoleta code.
- Support cache-only analysis reads, queued fetches, wait-until-ready fetches,
  and stale-while-revalidate refreshes.
- Support submitted-date day windows as first-class metadata requests.
- Prevent consumers from needing to understand Huldra's SQLite schema.
- Keep the implementation modern and boring: Pydantic models, httpx, FastAPI,
  Typer, SQLite/WAL, feedparser, pytest, respx/httpx mock transports.

## Non-Goals

- Do not import Recoleta modules or expose Recoleta types.
- Do not cache PDFs, source archives, paper HTML, or full text.
- Do not add a multi-machine distributed limiter in this proposal.
- Do not bypass arXiv limits with browser automation, fake identities, proxies,
  or multiple upstream clients.
- Do not make Huldra responsible for Recoleta's ingest watermarks, pipeline
  metrics, trend generation, or workflow gating.

## Boundary

### Huldra Owns

- `ArxivRequest`, `ArxivResult`, `ArxivPaper`, and compatibility-safe JSON
  serialization.
- Client-facing response models must tolerate additive response fields. For V1,
  this proposal uses permissive response parsing rather than parallel versioned
  endpoints.
- Request normalization and cache keys.
- Durable rate state, cooldown state, leases, queue claims, and worker state.
- Atom fetching and parsing.
- Cache integrity checks.
- Generic cache and maturity calculation:
  - raw cache completion;
  - complete paper match rows;
  - submitted-date maturity cutoff.
- Serving-mode enforcement:
  - `raw_completed` returns readable completed cache;
  - `analysis_ready` returns papers only when the cache is readable and mature.
- Generic maintenance commands:
  - one-shot sync for explicit requests/windows;
  - backfill for historical submitted-date windows;
  - foreground worker for queued and configured proactive windows.

### Recoleta Owns

- Reading Recoleta settings and fleet manifests.
- Translating Recoleta queries, workflow periods, and watermarks into Huldra
  requests.
- Enforcing Recoleta's production rule that immature or unavailable arXiv pool
  windows do not emit `ItemDraft` rows.
- Choosing Recoleta workflow policy: `strict`, `warn`, `off`, and unsafe
  immature-window overrides.
- Preserving Recoleta source-pull diagnostics and metrics.
- Advancing or preserving Recoleta source watermarks.
- Blocking or continuing workflows in `strict`, `warn`, or `off` modes.

## Public Contract V1

The existing `POST /v1/requests` and `HuldraClient.ensure(...)` remain the
canonical request surface. Maintenance commands may provide higher-level
helpers, but they should still reduce to the same request model.

### Request Shape

```json
{
  "client_id": "recoleta:embodied_ai",
  "search_query": "cat:cs.AI AND all:agent",
  "id_list": [],
  "sort_by": "submittedDate",
  "sort_order": "descending",
  "start": 0,
  "max_results": 60,
  "submitted_start": "2026-05-20T00:00:00+00:00",
  "submitted_end": "2026-05-21T00:00:00+00:00",
  "cache_policy": "cache_only",
  "readiness": "analysis_ready",
  "maturity_lag_days": 1,
  "priority": 0,
  "timeout_seconds": 30,
  "api_family": "legacy_search"
}
```

Rules:

- `submitted_start` and `submitted_end` define a half-open UTC window.
- Submitted-date bounds must be normalized to UTC minute precision before cache
  key generation and arXiv query construction. V1 should reject non-minute-aligned
  bounds rather than silently widening or narrowing a caller's window.
- Huldra converts that window to arXiv's inclusive `submittedDate` clause.
- `cache_policy` is not part of the cache key.
- `readiness` is not part of the cache key. It controls how a caller wants a
  completed cache interpreted.
- `readiness=raw_completed` returns readable completed cache even when a
  submitted-date window is not mature.
- `readiness=analysis_ready` returns papers only when the cache is readable and
  the submitted-date window is mature.
- `maturity_lag_days` is not part of the cache key. It controls how a completed
  submitted-date window is interpreted for this caller.
- `maturity_lag_days <= 0` disables maturity blocking for compatibility:
  `maturity_applicable=false`, `mature=true`, `analysis_ready=cache_readable`,
  and `maturity_cutoff=null`, including for current-day submitted-date windows.
- If `maturity_lag_days` is omitted, Huldra uses its configured default.
- Downstream clients still own their workflow policy and blocking behavior.
- Query windows must be normalized consistently so two clients asking for the
  same arXiv API request share the same cache entry.

### Result Shape

```json
{
  "serving_mode": "analysis_ready",
  "status": "ready",
  "cache_key": "huldra:v1:...",
  "request_id": null,
  "papers": [],
  "papers_total": 0,
  "cached_papers_total": 0,
  "total_results": 0,
  "cache_hit": true,
  "stale": false,
  "cache_readable": true,
  "mature": true,
  "ready": true,
  "analysis_ready": true,
  "maturity_applicable": true,
  "maturity_cutoff": "2026-05-21T00:00:00+00:00",
  "cooldown_until": null,
  "blocked_reason": null,
  "error_category": null,
  "error_message": null,
  "completed_at": "2026-05-21T00:05:00+00:00",
  "queued_at": null,
  "upstream_status": 200
}
```

Facts and serving mode:

- `serving_mode` echoes the requested `readiness` value.
- `status` describes whether Huldra served this request under the requested
  mode. It is not the same as analysis readiness.
- `ready` is a backward-compatible alias for "accepted by the requested serving
  mode"; it must be true whenever papers in this response are safe to consume.
- `cache_readable` means the completed cache entry and paper matches are
  internally complete.
- `papers_total` is the number of papers exposed in this response. If Huldra
  needs to report the underlying completed cache count while suppressing papers
  for `analysis_ready`, it must use `cached_papers_total`.
- `maturity_applicable=false` is used for requests without a submitted-date
  window. In that case `mature=true` and `analysis_ready=cache_readable`.
- For submitted-date windows, `analysis_ready` must equal
  `cache_readable && mature`.
- `blocked_reason` reports the factual reason the cache is not analysis-ready,
  even when `readiness=raw_completed` returns papers. For example, an immature
  raw read can have `status=ready`, `papers_total>0`, `mature=false`,
  `analysis_ready=false`, and `blocked_reason=immature_window`.

Result status vocabulary:

- `ready`: cache is readable and accepted by the requested readiness mode.
- `immature`: cache is readable, but a submitted-date window is not mature for
  `analysis_ready`.
- `cache_miss`: no completed readable cache exists.
- `queued`: work is queued for a worker.
- `cooling_down`: work is queued but global cooldown is active.
- `failed`: non-retryable failure for the cache key.
- `timeout`: `wait_until_ready` timed out.

Serving precedence:

- Cache integrity and maturity are evaluated before stale serving status.
- `stale` is a flag, not a V1 serving status. If a completed cache is accepted by
  the requested readiness mode and refresh work is queued, return `status=ready`,
  `ready=true`, and `stale=true`.
- For `readiness=analysis_ready` plus `cache_policy=stale_while_revalidate`, an
  immature completed window must return `status=immature`, `stale=true`,
  `analysis_ready=false`, `blocked_reason=immature_window`, and an empty
  `papers` list while still enqueueing refresh work.

For `readiness=analysis_ready`, Huldra must make misuse difficult:

- return `analysis_ready=false` and `blocked_reason=immature_window` for immature
  windows;
- return an empty `papers` list when status is `immature`;
- keep `readiness=raw_completed` available for exploratory clients that want a
  warm same-day cache. Raw reads may return papers, but they must not set
  `analysis_ready=true` for immature windows.

Huldra must not implement Recoleta's `strict`, `warn`, or `off` workflow policy.
It only reports cache and maturity facts, plus the serving-mode result requested
by the caller.

### Result Endpoint Semantics

`GET /v1/results/{cache_key}` is a raw cache inspection endpoint. It must not use
the originally stored request's `readiness` or `maturity_lag_days` to reinterpret
results, because those fields are caller-specific and excluded from the cache
key.

Raw inspection response rules:

- Raw inspection uses a separate response DTO, `ArxivRawInspectionResult`, rather
  than `ArxivResult`.
- `serving_mode` is `raw_inspection`.
- `cache_readable` reports cache integrity.
- `maturity_applicable`, `mature`, `maturity_cutoff`, `analysis_ready`, and
  `blocked_reason` are omitted or `null` unless the endpoint later accepts
  explicit caller interpretation parameters.
- `papers` may be returned when `cache_readable=true`.
- This endpoint is for debugging and low-level cache inspection, not Recoleta
  ingest.

Readiness-aware reads must use one of these surfaces:

- `POST /v1/requests` with the caller's full `ArxivRequest`;
- `HuldraClient.ensure(...)`;
- a future `POST /v1/results/interpret` endpoint if a cache-key-only workflow
  needs caller-specific interpretation.

Tests must cover both insertion orders:

- raw request fills the cache, then analysis-ready caller reads the same cache;
- analysis-ready request fills the cache, then raw caller reads the same cache.

The caller's requested serving mode must determine the interpretation in both
orders.

## Public Maintenance Surface V1

The CLI should remain a human-facing wrapper around typed Python client methods
and HTTP endpoints. Recoleta should not shell out to `huldra`.

### Python Client

```python
class HuldraClient:
    def sync_windows(
        self,
        requests: list[ArxivRequest],
        *,
        wait: bool = False,
        wait_timeout_seconds: float | None = None,
    ) -> HuldraMaintenanceResult: ...

    def backfill_windows(
        self,
        *,
        search_queries: list[str],
        start_date: date,
        end_date: date,
        max_results: int,
        wait: bool = False,
        wait_timeout_seconds: float | None = None,
        client_id: str = "huldra-backfill",
    ) -> HuldraMaintenanceResult: ...
```

### HTTP API

- `POST /v1/sync`: accepts explicit `ArxivRequest` objects plus optional
  `wait_timeout_seconds` and returns a maintenance summary.
- `POST /v1/backfill`: accepts query strings, inclusive dates, `max_results`,
  optional wait behavior, and optional `wait_timeout_seconds`, then returns a
  maintenance summary.

Maintenance result shape:

```json
{
  "requested_total": 3,
  "queued_total": 1,
  "cache_miss_total": 1,
  "cache_hit_total": 2,
  "completed_windows_total": 2,
  "upstream_requests_total": 1,
  "upstream_429_total": 0,
  "retry_after_seconds": null,
  "cooldown_active_total": 0,
  "skipped_windows_total": 0,
  "rate_limited_windows_total": 0,
  "failed_windows_total": 0,
  "papers_total": 120,
  "cooldown_active": false,
  "cooldown_until": null,
  "requests": [
    {
      "cache_key": "huldra:v1:...",
      "request_id": "uuid",
      "search_query": "cat:cs.AI",
      "submitted_start": "2026-05-20T00:00:00+00:00",
      "submitted_end": "2026-05-21T00:00:00+00:00",
      "raw_cache_status": "completed",
      "serving_status": "ready",
      "cache_hit": false,
      "upstream_status": 200,
      "cooldown_until": null,
      "error_category": null,
      "error_message": null,
      "papers_total": 60
    }
  ]
}
```

Counter semantics should stay close to Recoleta's existing
`ArxivPoolSyncResult` so Recoleta can preserve machine-readable diagnostics
without before/after inference. Counters count only work in this maintenance
call, not global broker totals.

Counter naming intentionally matches Recoleta's `ArxivPoolSyncResult` where the
meaning is equivalent:

- `requested_total` maps to Recoleta `requested_windows_total`;
- `completed_windows_total`;
- `cache_hit_total`;
- `cache_miss_total`;
- `upstream_requests_total`;
- `upstream_429_total`;
- `retry_after_seconds`;
- `cooldown_active_total`;
- `skipped_windows_total`;
- `rate_limited_windows_total`;
- `failed_windows_total`;
- `papers_total`.

Maintenance request coercion:

- Maintenance surfaces accept `ArxivRequest` objects for request identity, query
  shape, priority, timeout, and client attribution.
- Maintenance completion is raw cache completion: a target is complete when the
  cache is readable as `raw_completed`, even if the caller's requested
  `readiness=analysis_ready` would currently interpret the window as immature.
- Maintenance must not honor caller `cache_policy=cache_only` as "do not fetch".
  It should either reject unsupported cache policies up front or internally
  coerce the fetch path to enqueue/wait semantics. V1 should use internal
  coercion so Recoleta can pass ordinary request objects.
- Per-request maintenance entries must expose `raw_cache_status` separately from
  `serving_status`. `raw_cache_status` uses cache/worker vocabulary such as
  `completed`, `missing`, `failed`, `rate_limited`, `queued`, or `skipped`.
  `serving_status` is the optional result of interpreting that cache with the
  caller's requested `readiness`.

Maintenance attribution rules:

- `requested_total`: every input request.
- `cache_hit_total`: readable cache existed at this call's first evaluation of
  that target.
- `cache_miss_total`: readable cache did not exist at first evaluation.
- `queued_total`: this call created or joined pending work for the target cache
  key.
- Per-request entries include `joined_existing_queue: true` when work already
  existed for the target cache key.
- `upstream_requests_total`: only upstream requests executed by this maintenance
  call's inline drain.
- If another worker completes joined work while this call waits,
  `completed_windows_total` may increment for this call, but
  `upstream_requests_total` must not.
- `papers_total`: papers in target cache entries that are readable by the end of
  this maintenance call.

Wait behavior:

- `wait=False`: enqueue missing work and return immediately.
- `wait=True`: the calling process drains this request set through the same
  broker, queue, limiter, and fetcher used by normal workers. A separate worker
  may also run, but it is not required for correctness.
- The wait path must track the target request IDs/cache keys for this maintenance
  call. It must not define completion as "the global queue is empty".
- Queue claiming for maintenance wait should be able to claim target items by
  request ID or cache key. Existing unrelated queue items may remain queued and
  must not alter this call's counters.
- `wait=True` returns when every requested item is completed, cache-hit,
  rate-limited/cooling down, failed, skipped by active cooldown, or the selected
  wait timeout expires.
- Timeout precedence is explicit: use maintenance-level `wait_timeout_seconds`
  when provided; otherwise use the maximum non-null `ArxivRequest.timeout_seconds`
  in the request set; otherwise use Huldra's configured request timeout. Timeout
  is measured for this maintenance call, not for the global queue.
- Every request in the response must include a terminal or current state so a
  caller can decide whether cache-only ingest can proceed.

The CLI commands `huldra sync` and `huldra backfill` must call these public
surfaces rather than implementing a separate path.

## Required Huldra Behavior

### Stale Refresh

`stale_while_revalidate` must actually fetch upstream when a completed cache
exists. A worker may skip a completed cache only for normal queue items. Refresh
queue items must call the fetcher under the durable limiter, update the cache on
success, and preserve the previous completed cache on HTTP 429 or transient
failure.

Refresh intent must be durable:

- Add a queue work marker such as `queue_items.work_kind` with V1 values
  `fetch_missing` and `refresh_completed`, or an equivalent durable
  `refresh_requested` field.
- `stale_while_revalidate` enqueue records `refresh_completed` even when a
  completed cache entry already exists.
- If a normal pending item exists for the same cache key, a stale request may
  promote it to refresh work.
- The worker's completed-cache shortcut applies only to `fetch_missing` items.
- If a worker restarts before claiming the refresh item, the refresh intent must
  still be recoverable from the queue row.

### Cache Integrity

Completed cache reads must verify:

- `cache_entries.status = completed`;
- match row count equals `cache_entries.result_count`;
- every matched `arxiv_id` joins to a paper row;
- sort positions are deterministic.

If integrity fails, the cache entry is not readable and should be repaired by
refresh/backfill rather than served as a ready result.

For `legacy_search` V1, readiness is readiness of the requested result slice,
not proof that every matching arXiv record in the submitted-date window has been
cached. If `total_results > max_results`, `analysis_ready=true` means the
requested top-N slice is cache-readable and mature. A later contract may add a
`truncated` field if consumers need full-window completeness.

### ID-List Reuse

Huldra should avoid upstream requests when the requested papers already exist in
the paper cache.

Initial scope:

- exact normalized arXiv ID lookups, preserving every ID form already accepted
  by `normalize_arxiv_id()`, including old-style slash IDs such as
  `hep-th/9901001v1`;
- paper-cache composition applies only to pure ID-list requests:
  `search_query` absent, `submitted_start`/`submitted_end` absent, `start=0`,
  and default sort fields;
- for pure ID-list composition, `max_results` must be greater than or equal to
  the number of requested IDs; otherwise Huldra should reject the request or use
  the upstream path, but must not silently return a partial paper-cache response;
- mixed `search_query + id_list` requests and non-default pagination/sort
  requests do not use the paper-cache shortcut in V1;
- deterministic result composition in the caller's requested order;
- upstream fetch only for missing IDs;
- set-level in-flight reservation for missing normalized IDs so concurrent
  `[A, B]` and `[B, A]` requests do not issue duplicate upstream fetches while
  the paper cache is still cold;
- completed response cache recorded after composition.

Reservation algorithm:

- Add an `id_fetch_reservations` table keyed by normalized arXiv ID with
  `owner_token`, `request_id`, `acquired_at`, and `expires_at`.
- Before fetching missing IDs, acquire reservations for every missing normalized
  ID in a single transaction.
- If all missing IDs are reserved by this worker, fetch them in one upstream
  `id_list` request and release reservations after cache write or terminal
  failure.
- If some missing IDs are already reserved by another worker, delay the caller's
  queue item until the earliest reservation expiry or until the reserving worker
  completes.
- For overlapping sets such as `[A, B]` and `[B, C]`, a worker may fetch only
  the IDs it successfully reserved and leave the queue item delayed until the
  remaining IDs are cached or claimable.
- Expired reservations are reclaimable, matching the existing queue-claim
  recovery model.

Future scope:

- versionless ID aliases such as `2401.00001` resolving to the latest known
  versionful row.

### Proactive Window Maintenance

Huldra should offer generic maintenance surfaces so each downstream project does
not reimplement Recoleta's pool worker:

- `huldra sync`: enqueue and optionally wait for explicit requests/windows;
- `huldra backfill`: plan daily submitted-date windows for a date range;
- `huldra worker`: drain the queue and optionally maintain configured lookback
  windows.

These commands should use the same broker, queue, limiter, and fetcher codepath
as the HTTP API. No separate rate limiter or special fetch path is allowed.

### Lease and Request Reservation

The upstream lease must cover the full time from rate-limit wait through the
HTTP request and cache write. If a worker has to sleep before the next upstream
request, the lease timeout must be at least:

```text
wait_seconds + request_timeout_seconds + write_grace_seconds
```

Alternatively, Huldra can implement a durable `reserved_request_at` state so
workers reserve the next legal upstream slot without holding a lease while idle.
The first implementation may use the longer lease timeout because it is simpler
and keeps one-machine behavior correct.

## Implementation Runbook

This runbook is meant for a local Codex agent. All implementation work below
must happen in one Huldra PR. Each numbered implementation step ends with
exactly one commit after that step's checks pass.

### Step 0: Prepare

```bash
cd /Users/chenmohan/gits/huldra
git checkout -b codex/recoleta-compatible-broker
uv sync --dev
uv run ruff check .
uv run pyright
uv run pytest -q
```

Do not commit Step 0 unless the branch setup itself needs repository files
changed.

### Step 1: Add Contract Types and Compatibility Scaffolding

Add the public fields and types needed by later behavior changes without
changing fetch behavior yet:

- request-level `maturity_lag_days`, excluded from cache keys;
- `serving_mode`, `cache_readable`, and `mature` fields on `ArxivResult`;
- separate `ArxivRawInspectionResult` for `GET /v1/results/{cache_key}`;
- `HuldraMaintenanceResult` and request models for sync/backfill public
  surfaces;
- raw cache inspection semantics for `GET /v1/results/{cache_key}`;
- route/client method stubs that return validation-friendly empty maintenance
  summaries;
- focused tests that prove these models serialize and the existing API remains
  backward compatible;
- client-facing response models tolerate additive response fields for response
  DTOs while request DTOs can remain strict.

Do not add stale-refresh, cache-integrity, or safe-readiness assertions in this
step. Those assertions belong in the steps that implement the behavior, so each
step can pass and be committed independently.

Checks:

```bash
uv run pytest tests/test_models.py tests/test_api.py tests/test_client.py -q
uv run ruff check .
uv run pyright
```

Commit:

```bash
git add .
git commit -m "feat: add recoleta broker contract types"
```

### Step 2: Fix Stale Refresh and Cache Integrity

Implement:

- tests in `tests/test_cache_integrity.py` for unreadable completed caches with
  missing match rows or missing paper rows;
- worker tests proving stale-while-revalidate calls the fetcher when completed
  cache exists and preserves old cache on 429/transient failure;
- a durable refresh work marker such as `queue_items.work_kind` and tests for
  normal-to-refresh promotion, normal request after refresh exists, and worker
  restart before claim;
- broker tests proving `analysis_ready + stale_while_revalidate + immature
  submitted-date window` queues refresh work but returns no papers;
- a queue-item refresh path that bypasses the completed-cache worker shortcut
  for `cache_policy=stale_while_revalidate`;
- a store helper that returns completed cache only when match rows and paper
  rows are complete;
- broker reads through that helper instead of raw `get_cached_papers`;
- refresh failure preserves the previous completed cache status and paper
  matches.

Avoid duplicating fetch logic. The refresh path should call the same fetcher and
limiter used by normal queued requests.

Checks:

```bash
uv run pytest tests/test_worker.py tests/test_broker.py tests/test_cache_integrity.py -q
uv run ruff check .
uv run pyright
```

Commit:

```bash
git add .
git commit -m "fix: refresh completed arxiv cache safely"
```

### Step 3: Add Safe Analysis-Ready Serving Semantics

Implement:

- tests in `tests/test_recoleta_contract.py` for current-day immature windows,
  yesterday's mature windows, caller readiness on cache hit, and `cache_only`
  misses;
- tests proving request-level `maturity_lag_days` controls maturity evaluation
  without changing the cache key;
- tests proving `maturity_lag_days=0` makes current-day completed windows
  analysis-ready without changing cache identity;
- tests proving `raw_completed` current-day reads return papers while still
  reporting `mature=false`, `analysis_ready=false`, and
  `blocked_reason=immature_window`;
- tests proving `papers_total` counts only papers exposed in the response and
  `cached_papers_total` carries suppressed cache counts when present;
- tests proving raw-first and analysis-first cache fills do not affect the
  caller-specific readiness returned by later `POST /v1/requests` reads;
- `readiness=analysis_ready` does not expose immature papers by default;
- `raw_completed` remains available for exploratory same-day consumers;
- no `include_immature_papers` escape hatch in V1; callers that want warm
  same-day papers must explicitly request `raw_completed`;
- response JSON makes maturity cutoff and blocked reason visible.

Do not hard-code Recoleta behavior. Huldra should expose a generic readiness
contract that any client can consume.

Checks:

```bash
uv run pytest tests/test_readiness.py tests/test_recoleta_contract.py -q
uv run ruff check .
uv run pyright
```

Commit:

```bash
git add .
git commit -m "feat: enforce analysis-ready cache reads"
```

### Step 4: Reuse Cached Papers for ID Lists

Implement:

- exact normalized ID lookup from `papers` before enqueueing upstream work;
- composition of all-cached ID-list results in request order;
- upstream fetch only for missing IDs;
- a storage migration for `id_fetch_reservations` and set-level in-flight
  reservation for missing normalized IDs;
- response cache recording after composed ID-list reads;
- tests for reordered ID lists, partial misses, supported modern versioned IDs,
  old-style slash IDs, unsupported ID forms, and concurrent cold-cache `[A, B]`
  / `[B, A]` requests;
- tests proving only pure ID-list requests use paper-cache composition; mixed
  query+ID requests, nonzero `start`, non-default sort fields, and too-small
  `max_results` are rejected or use the upstream path according to the V1 rule.

Preserve exact response ordering. Do not simply sort `id_list` in the cache key
unless the API contract explicitly says result ordering is set-like.

Checks:

```bash
uv run pytest tests/test_keys.py tests/test_broker.py tests/test_client.py -q
uv run ruff check .
uv run pyright
```

Commit:

```bash
git add .
git commit -m "feat: reuse cached papers for id lists"
```

### Step 5: Add Generic Window Sync and Backfill Surfaces

Implement a small planner module rather than embedding date math in the CLI:

- `huldra.planner.build_submitted_date_windows(...)`;
- `HuldraClient.sync_windows(...)` and `HuldraClient.backfill_windows(...)`,
  including `wait_timeout_seconds`;
- `POST /v1/sync` and `POST /v1/backfill`;
- `huldra sync --search-query ... --date YYYY-MM-DD --max-results N --wait`;
- `huldra backfill --search-query ... --start-date YYYY-MM-DD --end-date YYYY-MM-DD`;
- optional repeated `--search-query` support;
- maintenance request coercion so caller `cache_policy` and `readiness` do not
  change raw cache completion criteria;
- `wait=True` inline drain behavior that does not require an external worker;
- target-set wait behavior that is not blocked by unrelated queued work;
- tests for `cache_policy=cache_only` coercion, current-day
  `readiness=analysis_ready` maintenance completion, and an unrelated queued item
  already present before `sync_windows(wait=True)`;
- tests for `joined_existing_queue`, attribution when another worker completes
  joined work, and `upstream_requests_total` counting only inline drain fetches;
- JSON output with Recoleta-parity counter names and per-request/window state.

All generated work must be ordinary `ArxivRequest` rows. The worker and API must
share the same queue and limiter.

Checks:

```bash
uv run pytest tests/test_cli_worker.py tests/test_cli_query.py tests/test_queue.py tests/test_api.py tests/test_client.py tests/test_e2e_multi_client.py -q
uv run ruff check .
uv run pyright
```

Commit:

```bash
git add .
git commit -m "feat: add arxiv window sync commands"
```

### Step 6: Harden Lease Timing and Worker Diagnostics

Implement:

- lease timeout calculation that covers `wait_seconds + request_timeout_seconds`
  for each fetch attempt;
- worker diagnostics for refresh vs normal fetch;
- status counters for unreadable cache entries and refresh failures if useful;
- regression test where `request_interval_seconds` is greater than the default
  lease timeout.

Checks:

```bash
uv run pytest tests/test_limiter.py tests/test_worker.py tests/test_metrics.py -q
uv run ruff check .
uv run pyright
```

Commit:

```bash
git add .
git commit -m "fix: keep upstream lease valid through waits"
```

### Step 7: Documentation and Release Notes

Update:

- `README.md` with the Recoleta-compatible contract and safe readiness usage;
- `docs/operations/api-examples.md` with submitted-date and cache-only examples;
- `docs/operations/local-daemon.md` with worker/sync/backfill supervision notes.

Checks:

```bash
uv run ruff check .
uv run pyright
uv run pytest -q
```

Commit:

```bash
git add .
git commit -m "docs: document recoleta-compatible broker contract"
```

### Step 8: PR Verification

Before opening the PR:

```bash
uv run ruff check .
uv run pyright
uv run pytest -q
git log --oneline --decorate -n 12
```

PR checklist:

- The PR contains all implementation commits from this runbook.
- Each step maps to one commit.
- The PR description includes the commands run.
- The PR calls out any intentional deviations from this proposal.
- The PR does not include Recoleta code changes.

## Acceptance Criteria

- Two equivalent Recoleta submitted-date window requests dedupe to one upstream
  arXiv API request.
- Submitted-date windows are canonicalized at UTC minute precision, with
  non-minute-aligned bounds rejected or otherwise handled by an explicitly tested
  canonicalization rule.
- A completed current-day window is not served as analysis-ready by default.
- `maturity_lag_days=0` disables maturity blocking, makes current-day completed
  windows analysis-ready, and does not change cache identity.
- `analysis_ready + stale_while_revalidate` never exposes immature papers; stale
  refresh is reported as a flag when maturity blocks serving.
- A raw completed current-day read may return papers, but still reports
  `mature=false`, `analysis_ready=false`, and `blocked_reason=immature_window`.
- Caller-specific readiness does not depend on which request first filled the
  cache.
- `sync_windows(wait=True)` can complete a Recoleta-style pre-sync without an
  external worker.
- `sync_windows(wait=True)` completion and counters are scoped to this request
  set, even when unrelated queue items already exist.
- `sync_windows(wait=True)` uses explicit timeout precedence:
  `wait_timeout_seconds`, then request-level timeout, then Huldra default.
- Maintenance sync/backfill complete raw readable cache entries and report
  `raw_cache_status` separately from caller `serving_status`.
- Additive response fields do not break the public Python client.
- Maintenance results expose Recoleta-parity counters and per-request/window
  states for the current call.
- Maintenance results include queue-join attribution and count upstream requests
  only when this call's inline drain executes the fetch.
- Stale refresh actually performs an upstream request and preserves old cache on
  refresh failure.
- Stale refresh intent is durable and survives queue dedupe, normal pending work,
  and worker restarts.
- ID-list requests can be satisfied from cached paper metadata when possible.
- ID-list paper-cache reuse applies only to pure ID-list requests in V1.
- ID-list reuse preserves current old-style slash ID behavior.
- `analysis_ready=true` means the requested legacy-search result slice is ready,
  not that all matching arXiv records beyond `max_results` were cached.
- Concurrent cold-cache ID-list requests for the same ID set do not duplicate
  upstream fetches.
- HTTP 429 creates a durable cooldown and suppresses later upstream probes until
  expiry.
- Multiple local workers cannot issue concurrent upstream requests.
- The full Huldra test suite, Ruff, and Pyright pass.
