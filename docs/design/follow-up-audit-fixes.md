# Follow-up Audit Fixes

Date: 2026-05-21

This note records the follow-up audit fixes applied after the initial Huldra
MVP commit.

## Fixes

- Cache hit readiness now uses the caller request when a caller asks for
  `analysis_ready` or `raw_completed`. The stored cache request remains a
  fallback for direct `GET /v1/results/{cache_key}` reads.
- The paper API supports old-style arXiv IDs with slashes, such as
  `hep-th/9901001v1`, through `/v1/papers/{arxiv_id:path}`. The Python client
  URL-encodes paper IDs before requesting them.
- `upstream_429_total` is now a durable cumulative counter, separate from
  `consecutive_429_total`. A successful request clears only the consecutive
  count.
- Worker pass outcomes now update heartbeat, next wake, and error diagnostics
  for cache hits, cooldown blocks, lease blocks, HTTP 429, transient failures,
  non-retryable failures, and successful completions.
- E2E tests now use `httpx.MockTransport` with the real `ArxivApiFetcher` to
  prove equivalent requests dedupe to one GET and cooldown suppresses upstream
  GETs until expiry.

## Known Process Exception

The initial MVP implementation was committed as one commit instead of one
commit per original runbook step. This branch keeps that commit and adds
follow-up fixes as separate commits. If exact step-by-step history is required
for review, rebuild a replacement branch from `main` and replay the original
runbook steps as individual commits.
