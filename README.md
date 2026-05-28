# Huldra

Huldra is a local arXiv metadata broker for one machine. Programs that need
arXiv papers can ask Huldra for metadata instead of each calling arXiv
directly. Huldra shares a SQLite cache, request queue, durable rate limiter,
cooldown state, and upstream lease across those programs.

Huldra is an independent package and CLI. It is not a plugin for another
project, and it does not depend on [Recoleta](https://github.com/NeapolitanIcecream/recoleta).

## Install

Install the published package:

```bash
pip install huldra-arxiv
huldra --help
```

For local development:

```bash
uv sync --dev
uv run huldra --help
```

The PyPI package name is `huldra-arxiv`. The Python package and CLI command are
still named `huldra`.

The default database is:

```text
~/.local/share/huldra/huldra.db
```

Override it per command with `--db PATH` or with `HULDRA_DB_PATH`.

## Run Locally

Initialize a store:

```bash
uv run huldra store init --db ~/.local/share/huldra/huldra.db
```

Start the local HTTP API:

```bash
uv run huldra daemon --db ~/.local/share/huldra/huldra.db --host 127.0.0.1 --port 8765
```

Run a foreground worker in a separate terminal:

```bash
uv run huldra worker --db ~/.local/share/huldra/huldra.db --poll-interval-seconds 300 --json
```

Check status:

```bash
uv run huldra status --db ~/.local/share/huldra/huldra.db --json
```

Status includes queue depth, cache totals, durable upstream 429 totals,
cooldown state, worker heartbeat, worker next wake, and the last worker error.

The API binds to `127.0.0.1` by default. Do not expose it to a public network
without a reverse proxy and authentication.

## CLI Query

Submit a query without waiting for the worker:

```bash
uv run huldra query \
  --db ~/.local/share/huldra/huldra.db \
  --client-id demo \
  --search-query 'cat:cs.AI AND all:agent' \
  --max-results 50 \
  --json
```

Read a completed result:

```bash
uv run huldra result --db ~/.local/share/huldra/huldra.db --cache-key KEY --json
```

`huldra result` is a raw cache inspection command. It reports whether the
stored cache entry is readable and returns cached papers when it can. It does
not reinterpret the cache for a caller's `analysis_ready` policy.

Look up one cached paper:

```bash
uv run huldra paper --db ~/.local/share/huldra/huldra.db --arxiv-id 2401.00001 --json
```

Sync a submitted-date UTC day and optionally wait for the worker path inline.
By default this completes one legacy search slice and reports
`coverage_status="slice"` even when arXiv says more results exist:

```bash
uv run huldra sync \
  --db ~/.local/share/huldra/huldra.db \
  --search-query 'cat:cs.AI AND all:agent' \
  --date 2026-05-20 \
  --max-results 60 \
  --wait \
  --json
```

Fetch every legacy search page for a bounded window by opting into complete
window mode:

```bash
uv run huldra sync \
  --db ~/.local/share/huldra/huldra.db \
  --search-query 'cat:cs.AI AND all:agent' \
  --date 2026-05-20 \
  --max-results 60 \
  --mode complete-window \
  --wait \
  --json
```

Backfill daily submitted-date windows:

```bash
uv run huldra backfill \
  --db ~/.local/share/huldra/huldra.db \
  --search-query 'cat:cs.AI' \
  --start-date 2026-05-01 \
  --end-date 2026-05-20 \
  --max-results 60 \
  --json
```

Run an OAI-PMH harvest for complete or category-scoped metadata sync:

```bash
uv run huldra harvest oai \
  --db ~/.local/share/huldra/huldra.db \
  --metadata-prefix arXiv \
  --set cs:cs:AI \
  --mode incremental \
  --json
```

## Python Client

```python
from huldra.client import HuldraClient

with HuldraClient(base_url="http://127.0.0.1:8765") as client:
    result = client.ensure_search(
        search_query="cat:cs.AI AND all:agent",
        max_results=50,
        wait=True,
    )
    print(result.status, result.papers_total)
```

For Recoleta-style pre-syncs, call the maintenance surface instead of shelling
out to the CLI:

```python
from datetime import UTC, datetime, timedelta

from huldra.client import HuldraClient
from huldra.models import ArxivRequest, CachePolicy, ReadinessMode

day = datetime(2026, 5, 20, tzinfo=UTC)
request = ArxivRequest(
    client_id="recoleta:embodied_ai",
    search_query="cat:cs.AI",
    submitted_start=day,
    submitted_end=day + timedelta(days=1),
    max_results=60,
    cache_policy=CachePolicy.CACHE_ONLY,
    readiness=ReadinessMode.ANALYSIS_READY,
)

with HuldraClient(base_url="http://127.0.0.1:8765") as client:
    summary = client.sync_windows([request], wait=True, wait_timeout_seconds=30)
    print(summary.completed_windows_total, summary.upstream_requests_total)
```

Maintenance completion means the raw cache is readable. The per-request
`serving_status` still tells you whether the same cache is currently accepted
by the request's readiness mode. For legacy search, check `coverage_status`,
`completed_slices_total`, `pages_total`, and `pages_completed_total` before
treating a window as complete.

## Safe Readiness

Use `readiness="analysis_ready"` for ingestion paths that must not consume
immature submitted-date windows. If a completed window is still inside the
maturity lag, Huldra returns:

- `status="immature"`
- `ready=false`
- `analysis_ready=false`
- `blocked_reason="immature_window"`
- an empty `papers` list
- `cached_papers_total` with the number of suppressed cached papers

Use `readiness="raw_completed"` for exploratory reads that may inspect same-day
metadata. Raw reads can return papers from an immature window, but they still
report `analysis_ready=false`, `mature=false`, and
`blocked_reason="immature_window"`.

Set request-level `maturity_lag_days=0` only when the caller explicitly wants
to disable maturity blocking. This field changes readiness interpretation; it
does not change the cache key.

Submitted-date bounds must be UTC minute-aligned. Huldra rejects bounds with
seconds or microseconds instead of silently widening or narrowing the window.

## HTTP API

```bash
curl http://127.0.0.1:8765/v1/status

curl -X POST http://127.0.0.1:8765/v1/requests \
  -H 'content-type: application/json' \
  -d '{"client_id":"demo","search_query":"cat:cs.AI","max_results":10}'
```

## Rate Limits And 429 Cooldown

Huldra keeps all arXiv legacy API access behind one durable limiter. The default
request interval is 5 seconds, which is more conservative than arXiv's 3 second
minimum. Only one upstream fetch lease can be held at a time.

When arXiv returns HTTP 429, Huldra persists `cooldown_until` in SQLite. New
requests can still be queued, but workers will not probe upstream again until
the cooldown expires.

## OAI-PMH Harvesting

The OAI-PMH surface uses `https://oaipmh.arxiv.org/oai` by default and stores
harvest jobs, page state, watermarks, raw OAI records, deleted headers, and
normalized paper metadata. Incremental harvests use the last successful server
response date or datestamp watermark unless `--from` is provided explicitly.
Watermarks advance only after all pages in the harvest succeed.

Use legacy search for request-sized slices and complete-window maintenance.
Use OAI-PMH for full mirrors, category-scoped mirrors, and datestamp-based
incremental sync.

## Metadata-Only Boundary

This package stores descriptive metadata from arXiv: IDs, titles, abstracts,
authors, categories, publication dates, comments, journal references, DOIs,
OAI identifiers, OAI datestamps, set specs, license fields, deleted-record
state, and raw metadata needed for reprocessing. It does not cache or serve
PDFs, source tarballs, generated full text, or paper HTML.

## Non-Goals

- No Recoleta dependency or runtime adapter.
- No PDF, source, or full-text cache.
- No multi-machine distributed limiter. For more than one machine, run one
  shared broker or add a future shared rate-state backend.
