# Huldra

Huldra is a local arXiv metadata broker for one machine. Programs that need
arXiv papers can ask Huldra for metadata instead of each calling
`export.arxiv.org` directly. Huldra shares a SQLite cache, request queue,
durable rate limiter, cooldown state, and upstream lease across those programs.

Huldra is an independent package and CLI. It is not a plugin for another
project, and it does not depend on Recoleta.

## Install

```bash
uv sync --dev
uv run huldra --help
```

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

Look up one cached paper:

```bash
uv run huldra paper --db ~/.local/share/huldra/huldra.db --arxiv-id 2401.00001 --json
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

## Metadata-Only Boundary

This MVP stores descriptive metadata from the arXiv API: IDs, titles,
abstracts, authors, categories, publication dates, comments, journal references,
DOIs, and small provenance fields. It does not cache or serve PDFs, source
tarballs, generated full text, or paper HTML.

## Non-Goals

- No Recoleta dependency or runtime adapter.
- No PDF, source, or full-text cache.
- No multi-machine distributed limiter. For more than one machine, run one
  shared broker or add a future shared rate-state backend.
- No OAI-PMH backend yet. The current backend is the legacy search API.
