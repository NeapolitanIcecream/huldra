# Changelog

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
