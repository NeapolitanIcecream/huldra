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

## Read A Result

```bash
curl http://127.0.0.1:8765/v1/results/huldra:v1:REPLACE_ME
```

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
