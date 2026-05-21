# Local arXiv Metadata Broker

## Problem

Multiple local programs may need the same arXiv metadata. If each program calls
`export.arxiv.org` on its own, they can duplicate requests, amplify HTTP 429
responses, and lose cooldown state between runs.

Huldra is a single-machine metadata broker. Programs submit equivalent queries,
ID lists, or submitted-date windows to Huldra. Huldra deduplicates those
requests through a shared cache key, stores request state in SQLite, and lets a
worker perform upstream fetches under one durable limiter.

This single-machine scope is intentional for the MVP.

## Broker State

Huldra keeps these state surfaces in one SQLite database:

- Shared cache: completed metadata responses and paper matches.
- Queue: pending work keyed by normalized request fingerprints.
- Durable limiter: `last_request_at` and upstream status.
- Cooldown: persisted `cooldown_until` after HTTP 429.
- Lease: one `upstream_fetch` holder so local workers do not open concurrent
  arXiv API connections.

This is the behavior that prevents two clients asking for the same query from
creating two upstream requests.

## Scope

The MVP is metadata-only. It stores descriptive metadata that arXiv allows to be
retrieved, stored, transformed, and shared. It does not cache or serve PDFs,
source files, paper HTML, or other e-print content.

The implemented backend family is `legacy_search`. It supports arXiv API
parameters such as `search_query`, `id_list`, `start`, `max_results`, `sortBy`,
and `sortOrder`. `max_results` is capped at 2000 for one upstream slice. Large
or full metadata harvesting belongs in a future OAI-PMH backend, not in search
API brute-force pagination.

OAI-PMH is reserved as a future backend for full metadata mirrors and
incremental sync. It is not implemented in this MVP.

## Single-Machine Boundary

SQLite, WAL, `BEGIN IMMEDIATE`, queue claims, and lease rows are enough for one
machine. They are not a distributed limiter. Multi-machine deployments must use
one centralized broker or a future shared rate-state backend.

## Recoleta Boundary

Huldra is independent. Core code does not load Recoleta modules, expose
Recoleta types, or require a Recoleta checkout. Future consumers can call
Huldra through the CLI, Python client, or local HTTP API.

## Acceptance

- Package and CLI are named `huldra`.
- CLI, Python client, and local FastAPI API can submit metadata requests.
- Equivalent request fingerprints deduplicate queue items and completed cache
  entries.
- All upstream requests share durable rate state, cooldown state, queue state,
  and an upstream lease.
- HTTP 429 persists `cooldown_until`; workers do not keep probing upstream while
  cooldown is active.
- Tests use fake fetchers or `httpx.MockTransport`, never the real arXiv API.
- The repository has no Python runtime imports from Recoleta in `src/` or
  `tests/`.
