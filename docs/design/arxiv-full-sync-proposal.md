# arXiv Complete Sync Proposal

Verified: 2026-05-28

## Summary

Huldra currently implements a local broker for arXiv legacy search API slices. It
deduplicates equivalent requests, rate-limits a shared upstream path, stores
metadata in SQLite, and supports request-driven stale refresh. That is useful for
query/window prefetching, but it is not a complete arXiv metadata sync or mirror.

This proposal separates two product surfaces:

- `legacy_search`: request and cache bounded search slices or explicitly complete
  search windows.
- `oai_pmh`: harvest arXiv metadata for complete or category-scoped mirrors with
  datestamp watermarks and resumption-token paging.

The immediate bug fix is to reject arXiv Atom error feeds before they enter the
paper store. The larger work is a staged implementation of complete search-window
pagination, an OAI-PMH backend, proactive incremental harvesting, and richer
metadata storage.

## Sources

- arXiv API User's Manual: https://info.arxiv.org/help/api/user-manual.html
- arXiv OAI-PMH harvester notes: https://info.arxiv.org/help/oa/index.html
- OAI-PMH 2.0 protocol: https://www.openarchives.org/OAI/openarchivesprotocol.html

Relevant upstream facts:

- The legacy API uses `start` and `max_results` for paging and exposes
  `opensearch:totalResults`, `startIndex`, and `itemsPerPage`.
- arXiv recommends smaller slices and points bulk metadata harvesting or set
  information to OAI-PMH.
- arXiv legacy API errors are also Atom feeds with one error entry.
- arXiv OAI-PMH now uses `https://oaipmh.arxiv.org/oai`, exposes several metadata
  formats including `arXiv` and `arXivRaw`, and is designed for complete copying
  and incremental synchronization by datestamp.
- OAI-PMH list responses may be incomplete and continued with `resumptionToken`.

## Verification

### 1. Sync and backfill are one-slice legacy search requests

Status: verified.

Current evidence:

- `src/huldra/keys.py` builds one request parameter set from one `ArxivRequest`.
  It passes through `start`, `max_results`, `sortBy`, and `sortOrder`.
- `src/huldra/worker.py` calls `fetcher.fetch(fetch_request)` once per queue item
  and records that response as a completed cache entry.
- `src/huldra/planner.py` creates one submitted-date request per query per day.
- `src/huldra/db.py` stores `result_count` and `total_results`, but cache status
  has no `partial`, `overflow`, or aggregate page-completion state.

Impact:

If `total_results > result_count` for a daily window, Huldra marks the cache entry
completed even though it only has one slice. This is acceptable for a slice cache,
but misleading for any caller that interprets `sync` or `backfill` as "all records
matching this day/query are present".

### 2. There is no OAI-PMH backend

Status: verified.

Current evidence:

- `ArxivRequest.api_family` only allows `legacy_search`.
- No `src/huldra/*oai*` module exists.
- README and `docs/design/local-arxiv-metadata-broker.md` explicitly list OAI-PMH
  as future work.

Impact:

Huldra cannot currently support full metadata mirroring, category-scoped
harvesting, or datestamp-based incremental updates in the way arXiv's OAI-PMH
interface is intended to support.

### 3. arXiv Atom error feeds can be parsed as papers

Status: verified bug.

Current evidence:

- `src/huldra/fetcher.py` treats every HTTP 2xx response as parseable Atom result
  data.
- `src/huldra/atom.py` maps every feed entry through `_paper_from_entry`.
- A local reproduction using arXiv's documented error-feed shape produces an
  `ArxivPaper` with `arxiv_id="api/errors"`, `title="Error"`, and author
  `"arXiv api core"`.

Impact:

Malformed requests can create bogus rows in `papers` and completed cache entries.
This should be fixed before adding broader sync paths.

### 4. Refresh is request-driven, not watermark-driven

Status: verified.

Current evidence:

- `stale_while_revalidate` queues `refresh_completed` only when a caller asks for
  an already completed cache entry.
- There is no table for upstream datestamp watermarks, saved harvest jobs, or
  scheduled incremental refresh state.
- `papers.updated_at` stores arXiv entry metadata, but no worker advances a
  `lastUpdatedDate` or OAI datestamp cursor from it.

Impact:

Metadata changes such as DOI, journal reference, replacement, withdrawal, license,
or administrative updates will only be seen if some later request happens to hit
the same cache key or paper IDs. This is cache refresh, not synchronization.

### 5. Stored metadata is useful but not complete

Status: verified.

Current evidence:

- `ArxivPaper` stores IDs, URL, title, abstract, author names, primary category,
  categories, published/updated timestamps, comment, journal reference, DOI, and a
  small `raw_atom` object.
- The SQLite `papers` table mirrors those fields.
- Current parser does not persist author affiliations, license, complete link
  details, OAI identifiers, OAI datestamps, OAI sets, deletion status, version
  history, or raw arXiv/arXivRaw XML.

Impact:

This is enough for many search-result consumers. It is not enough for a mirror or
for consumers that need provenance, update auditing, version history, license
state, or deleted/withdrawn record handling.

## Goals

- Preserve current legacy search behavior for existing callers.
- Make slice semantics explicit and prevent partial windows from looking complete.
- Add an OAI-PMH backend for complete and incremental metadata harvesting.
- Store enough normalized and raw metadata to support reprocessing without
  refetching.
- Add clear CLI/API names so "sync", "backfill", "slice", "window", and
  "harvest" cannot be confused.
- Keep all upstream access behind Huldra's durable limiter and diagnostics.

## Non-Goals

- Do not use legacy search pagination as the primary full-mirror backend.
- Do not cache PDFs, source tarballs, generated full text, or paper HTML.
- Do not change Recoleta watermarks or ingestion policy; expose metadata state for
  callers to consume.
- Do not run live arXiv calls in the test suite.

## Proposed Architecture

### Track A: Atom error-feed handling

Add explicit error detection before `ArxivPaper` construction.

Implementation shape:

- Add `ArxivAtomError` or `ParsedArxivFeed.errors`.
- Detect a legacy API error feed when all are true:
  - feed has exactly one entry;
  - entry title normalizes to `Error`;
  - entry id or alternate link has path `/api/errors` or URL fragment under that
    path;
  - the entry does not have a paper-style `http://arxiv.org/abs/...` id.
- In `ArxivApiFetcher.fetch`, convert parsed API errors into
  `NonRetryableFetchError` for request-shape errors.
- Preserve HTTP 429 and 5xx handling as-is.

Tests:

- `tests/test_atom.py`: documented Atom error feed is detected and not returned
  as paper data.
- `tests/test_fetcher.py`: HTTP 200 error feed raises `NonRetryableFetchError`.
- `tests/test_worker.py`: non-retryable error feed records failed cache state and
  does not insert `api/errors`.

### Track B: Complete legacy search windows

Keep one-slice requests as the default. Add an explicit complete-window mode for
maintenance commands that need all results up to a documented cap.

Data model additions:

- `sync_jobs`: logical job ID, mode (`slice`, `complete_window`), query/window
  parameters, status, coverage status, totals, timestamps.
- `sync_job_pages`: job ID, cache key, start, max_results, result_count,
  total_results, status, attempt diagnostics.
- `CacheEntry.coverage_status`: `slice`, `complete`, `partial`, `overflow`, or
  `unknown`.

Worker behavior:

1. Fetch the first page using the current path.
2. Compare `start + result_count` with `total_results`.
3. If more records remain and the caller requested `complete_window`, enqueue
   follow-up page requests with contiguous `start` offsets.
4. Mark the logical job complete only when all planned pages are readable and
   contiguous.
5. If the requested window exceeds the configured legacy cap, mark `overflow`
   instead of pretending it is complete.
6. For submitted-date windows, optionally split oversized windows into smaller
   time ranges before paging, because arXiv warns that large legacy result sets
   are expensive.

API/CLI:

- Keep `huldra query` as slice-oriented.
- Keep current `sync` and `backfill` defaults backward compatible, but rename
  result language to `completed_slices_total`.
- Add `--mode slice|complete-window` or `--complete-window`.
- Add `--split-overflow` for submitted-date windows.
- Return per-window `coverage_status`, `result_count`, `total_results`, and
  `pages_total`.

Tests:

- First page with `total_results > max_results` creates more page work in
  complete-window mode.
- Same response remains a completed `slice` in default mode but reports
  `coverage_status="slice"`.
- A missing or failed middle page keeps the aggregate window incomplete.
- An overflow beyond the configured cap reports `overflow` and does not expose
  `analysis_ready=true` for complete-window callers.
- Queue dedupe still works for repeated page requests.

### Track C: OAI-PMH backend

Add a separate backend instead of overloading `ArxivRequest`.

Models:

- `ApiFamily = Literal["legacy_search", "oai_pmh"]`.
- `OaiHarvestRequest`:
  - `client_id`
  - `metadata_prefix`: `arXiv` by default, `arXivRaw` optional
  - `set_spec`: optional, e.g. `cs:cs:AI`
  - `from_datestamp`: optional for explicit repair/replay
  - `until_datestamp`: optional for bounded audits, not default incrementals
  - `mode`: `initial` or `incremental`
  - `cache_policy`, `priority`, `timeout_seconds`
- `OaiHarvestResult`: job ID, status, records processed, papers upserted,
  deleted records, current watermark, resumption token state.

Tables:

- `oai_harvest_jobs`: request JSON, status, metadata prefix, set spec, counters,
  error fields, started/completed timestamps.
- `oai_watermarks`: `(metadata_prefix, set_spec)` key, last response date,
  last datestamp seen, last successful harvest ID.
- `oai_pages`: harvest ID, request URL params, resumption token hash, status,
  response date, records count.
- `oai_records`: OAI identifier, arXiv ID, metadata prefix, datestamp,
  deleted flag, raw XML or compressed raw payload, first/last seen timestamps.
- `paper_versions`: arXiv ID, version number, date, size/source flags when
  available from `arXivRaw`.
- `paper_authors`: arXiv ID, position, name, affiliation.
- `paper_categories`: arXiv ID, category, scheme, primary flag.
- optional `paper_links`: relation, title, href, content type.

Fetcher:

- `OaiPmhFetcher` with `Identify`, `ListMetadataFormats`, `ListSets`,
  `ListRecords`, and `GetRecord` helpers.
- Use `ListRecords&metadataPrefix=...` for initial and incremental harvests.
- Continue each incomplete list with `resumptionToken` until no token remains.
- Parse OAI `<error>` elements into retryable or non-retryable categories.
- Store deleted headers and propagate tombstone state.
- Use arXiv's current OAI base URL by default:
  `https://oaipmh.arxiv.org/oai`.

Watermark policy:

- Initial full harvest: request without a datestamp range by default, or from
  `earliestDatestamp` for repair scenarios.
- Incremental harvest: set `from` to the previous successful server response
  date or stored watermark. Do not set `until` by default.
- Commit the next watermark only after the whole harvest, including all
  resumption-token pages, succeeds.
- Keep a configurable overlap window for defensive replay; upserts and record
  uniqueness make overlap idempotent.

Tests:

- OAI parser handles normal `arXiv` records, `arXivRaw` records, deleted headers,
  OAI errors, and set specs.
- Resumption-token harvest stores page state and continues until exhausted.
- Failed page does not advance the watermark.
- Replayed overlap does not duplicate papers, versions, authors, or raw records.
- Migration tests cover existing SQLite databases.

### Track D: Proactive refresh

Introduce explicit jobs rather than relying on caller-triggered stale reads.

Legacy search jobs:

- Saved query/window definitions can be refreshed periodically.
- Use `lastUpdatedDate` sorting only as a bounded search optimization, not as the
  authoritative mirror watermark.
- Refresh known ID sets when callers want current metadata for a local subset.

OAI jobs:

- `huldra harvest oai --mode incremental` advances OAI watermarks.
- A daemon mode can schedule incremental harvests per `(metadata_prefix,
  set_spec)`.
- Status reports include last successful harvest, next scheduled attempt,
  current watermark, records processed, and latest error.

### Track E: Metadata enrichment

Keep the existing `ArxivPaper` response stable while adding richer optional
fields.

Response additions:

- `authors_detail`: ordered objects with `name` and optional `affiliation`.
- `license`: from OAI `arXiv` where available.
- `oai_identifier`, `oai_datestamp`, `oai_set_specs`.
- `links`: full link metadata.
- `versions`: normalized version history when populated from `arXivRaw`.
- `withdrawn` or `deleted`: explicit state for OAI deleted records or withdrawn
  metadata.
- `raw_metadata`: references to stored raw payloads, not necessarily inline by
  default.

Storage principles:

- Store normalized fields for common queries.
- Store raw XML for lossless reprocessing and future parser improvements.
- Prefer additive migrations and backward-compatible response defaults.

## Rollout Plan

### Milestone 0: Safety fix and vocabulary cleanup

- Implement Atom error-feed detection.
- Add tests proving no bogus `api/errors` paper is stored.
- Update docs to call current behavior "legacy search slice" where appropriate.

Acceptance:

- Error Atom feeds become failed/non-retryable requests.
- Existing successful search tests still pass.

### Milestone 1: Complete-window legacy pagination

- Add job/page tables and coverage status.
- Add explicit complete-window mode to API, Python client, and CLI.
- Preserve one-slice defaults.
- Add overflow and partial-state reporting.

Acceptance:

- A daily window with three pages completes only after all three pages are stored.
- A single fetched slice with more upstream results reports `slice` or `partial`,
  not complete-window success.

### Milestone 2: OAI-PMH read path

- Add OAI models, fetcher, parser, migrations, and harvest job execution.
- Support `arXiv` first, then `arXivRaw`.
- Store raw records and normalized paper fields.

Acceptance:

- Fixture-driven initial harvest can process multiple pages through a
  resumption token.
- OAI errors and deleted headers are represented correctly.

### Milestone 3: Incremental OAI harvesting

- Add watermark table and commit policy.
- Add CLI/API for `initial`, `incremental`, `status`, and repair/replay.
- Add scheduler or daemon loop integration.

Acceptance:

- Failed harvests do not advance watermarks.
- Successful incremental harvest resumes from the previous watermark and is
  idempotent under replay.

### Milestone 4: Rich metadata and consumer migration

- Add author affiliations, license, sets, versions, links, and raw payload
  references to responses.
- Document response differences between legacy search and OAI-PMH.
- Add migration notes for consumers that previously treated `sync` as complete.

Acceptance:

- Existing clients can still read the old fields.
- Mirror consumers can inspect provenance, license, versions, and OAI datestamp.

## Open Questions

- Should complete-window mode attempt automatic time-window bisection by default,
  or only report overflow and require the caller to choose a smaller window?
- Should raw XML be stored inline in SQLite or in content-addressed files with
  SQLite metadata references?
- What retention policy should apply to historical raw records after later OAI
  updates?
- Should OAI `arXivRaw` be mandatory for mirror mode, or optional because it is
  heavier than the `arXiv` metadata format?

## Recommended First Change

Start with Milestone 0. It is small, fixes a real data-corruption bug, and gives
the project a safer foundation before adding pagination and harvest state.
