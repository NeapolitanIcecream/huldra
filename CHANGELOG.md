# Changelog

## 0.4.0 - 2026-07-22

### Added

- Hard page, request, and runtime budgets for complete-window maintenance and
  OAI-PMH harvests, exposed consistently through the API, client, and CLI.
- Durable OAI page checkpoints, request accounting, deadlines, and same-scope
  leases so interrupted harvests resume without replaying committed pages.
- Persisted stale-while-revalidate freshness deadlines with atomic refresh
  reservation across broker processes.
- Low-cardinality request timing and rate-limit diagnostics in events and
  broker status.

### Changed

- HTTP 429 and OAI `503 + Retry-After` now share adaptive exponential cooldown
  while retaining separate durable counters. `Retry-After` is a hard lower
  bound, and random jitter only extends the wait.
- Complete-window and backfill planning reject work before queue expansion when
  their request budgets cannot cover the initial plan.
- Store schema advances to v7, adding durable upstream request budgets; normal
  store initialization upgrades prior schemas in place.

### Fixed

- Closed a limiter race by acquiring the shared upstream lease before reading
  durable cooldown state.
- Bounded malformed OAI pagination, including repeated tokens, token cycles,
  and pages that make no progress.
- Prevented a crash after the final OAI page checkpoint from refetching that
  page on restart.

## 0.3.0 - 2026-07-17

### Added

- Dry-run-first `store gc` retention for old diagnostic events, terminal queue
  items, and terminal sync jobs/pages while preserving active work and leases.
- Explicit `store vacuum` maintenance and status counters for event, queue,
  sync-job, and sync-page volume.

### Changed

- Idle worker passes are silent by default, worker JSON output is compact
  JSON Lines, and `--emit-idle` enables short-lived idle diagnostics.
- Worker polling now rejects intervals below one second.
- Daemon startup detects a healthy Huldra already using the endpoint, and the
  launchd guidance uses failure-only restart with throttling and bounded logs.

### Fixed

- Stopped persisting `worker_start` and `worker_stop` events for every worker
  pass, which caused the events table and captured idle output to grow without
  useful work.
- Made retention previews read-only so they do not create or migrate a target
  database.
- Reclaimed expired async sync/backfill jobs after all associated queue/cache
  work ages past the retention cutoff, while preserving jobs with active or
  recent work.
- Built daemon health-probe URLs safely for IPv6 bind literals.

## 0.2.0 - 2026-05-29

### Added

- Complete-window maintenance mode with sync job/page tracking while keeping
  default sync and backfill behavior on legacy search-slice semantics.
- OAI-PMH harvesting with OAI record/page storage, deleted headers, raw
  metadata, resume tokens, day-granular watermarks, and richer paper metadata.
- CLI, HTTP API, Python client, README, and operations documentation for OAI
  harvests and slice versus complete-window behavior.

### Changed

- Shared arXiv limiter state across OAI and legacy Atom fetches so mixed
  workloads respect the same upstream request delay and cooldowns.
- Reconciled mixed OAI/legacy records by preserving OAI provenance and
  tombstones, merging version-family rows in both directions, and resolving
  OAI base rows from versioned read paths.

### Fixed

- Rejected malformed OAI responses that previously could advance harvest
  watermarks, including missing metadata, blank identifiers, invalid
  datestamps, malformed records, and well-formed non-OAI responses.
- Resumed interrupted OAI harvests from saved resumption tokens and kept
  explicit bounded/replay harvests from mutating authoritative watermarks.
- Preserved versioned legacy rows when OAI tombstones arrive and prevented
  legacy refreshes from clearing OAI tombstones.

## 0.1.0 - 2026-05-22

First public release. The PyPI package is `huldra-arxiv`; the Python import
package and CLI command are `huldra`.

### Added

- Local arXiv metadata broker with a SQLite cache, request queue, durable rate limiter, upstream lease, and persisted 429 cooldown state.
- CLI commands for store setup, daemon startup, worker execution, status checks, query submission, result lookup, paper lookup, sync, and backfill.
- FastAPI HTTP API for submitting requests, reading status, fetching cached results, and running maintenance sync windows.
- Python client for query and maintenance workflows.
- Cache readiness modes for analysis-safe reads and raw completed cache inspection, including submitted-date maturity handling.
- Metadata-only storage for arXiv IDs, titles, abstracts, authors, categories, dates, comments, journal references, DOIs, and provenance fields.
- Release validation coverage across limiter behavior, cache keys, migrations, queueing, worker execution, API behavior, CLI flows, and multi-client cooldown handling.
