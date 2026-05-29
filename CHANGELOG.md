# Changelog

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
