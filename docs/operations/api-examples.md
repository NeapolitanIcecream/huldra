# Huldra API Examples

These examples assume the local API is running on `127.0.0.1:8765`.
All request and result examples deal with arXiv metadata only.

## Status

```bash
curl http://127.0.0.1:8765/v1/status
```

The response includes queue depth, cache counts, paper count, upstream request
count, and cooldown state.

## Submit A Search Query

```bash
curl -X POST http://127.0.0.1:8765/v1/requests \
  -H 'content-type: application/json' \
  -d '{
    "client_id": "example",
    "search_query": "cat:cs.AI AND all:agent",
    "max_results": 50
  }'
```

If the cache is missing, Huldra returns a queued result with a `cache_key` and
`request_id`. A worker must process the queue before papers are available.

## Submit An ID List

```bash
curl -X POST 'http://127.0.0.1:8765/v1/requests?wait=true' \
  -H 'content-type: application/json' \
  -d '{
    "client_id": "example",
    "id_list": ["2401.00001", "2401.00002"],
    "max_results": 2,
    "timeout_seconds": 30
  }'
```

## Cache-Only Analysis Read

Use `analysis_ready` when the caller must not consume immature submitted-date
windows.

```bash
curl -X POST http://127.0.0.1:8765/v1/requests \
  -H 'content-type: application/json' \
  -d '{
    "client_id": "recoleta:example",
    "search_query": "cat:cs.AI",
    "submitted_start": "2026-05-20T00:00:00+00:00",
    "submitted_end": "2026-05-21T00:00:00+00:00",
    "max_results": 60,
    "cache_policy": "cache_only",
    "readiness": "analysis_ready"
  }'
```

If the cache is complete but the window is not mature, the response has
`status="immature"`, `analysis_ready=false`, and an empty `papers` list.
`cached_papers_total` reports how many cached papers were suppressed.

Use `readiness="raw_completed"` to inspect the same completed cache without
blocking on maturity. Raw reads can return papers, but they still report the
maturity facts.

## Read A Result

```bash
curl http://127.0.0.1:8765/v1/results/huldra:v1:REPLACE_ME
```

This endpoint is for raw cache inspection. It returns
`serving_mode="raw_inspection"` and does not apply caller-specific readiness
settings.

## Sync Explicit Windows

```bash
curl -X POST http://127.0.0.1:8765/v1/sync \
  -H 'content-type: application/json' \
  -d '{
    "wait": true,
    "wait_timeout_seconds": 30,
    "requests": [
      {
        "client_id": "recoleta:example",
        "search_query": "cat:cs.AI",
        "submitted_start": "2026-05-20T00:00:00+00:00",
        "submitted_end": "2026-05-21T00:00:00+00:00",
        "max_results": 60,
        "cache_policy": "cache_only",
        "readiness": "analysis_ready"
      }
    ]
  }'
```

`wait=true` drains only the requested cache keys inline. Other queued work may
remain queued.

## Backfill Daily Windows

```bash
curl -X POST http://127.0.0.1:8765/v1/backfill \
  -H 'content-type: application/json' \
  -d '{
    "search_queries": ["cat:cs.AI", "cat:cs.LG"],
    "start_date": "2026-05-01",
    "end_date": "2026-05-07",
    "max_results": 60,
    "wait": false,
    "client_id": "huldra-backfill"
  }'
```

The maintenance response reports counters for this call, including
`requested_total`, `queued_total`, `cache_hit_total`, `cache_miss_total`,
`completed_windows_total`, `upstream_requests_total`, `upstream_429_total`, and
per-window `raw_cache_status` and `serving_status`.

## Read A Paper

```bash
curl http://127.0.0.1:8765/v1/papers/2401.00001v1
```

## Cooldown Behavior

When arXiv returns HTTP 429, Huldra stores `cooldown_until`. During cooldown,
new requests may enter the queue, but workers return a cooling-down state and
do not send another upstream request.

## Security

The API is intended for local software on the same machine. Do not bind it to a
public interface without authentication and network controls.
