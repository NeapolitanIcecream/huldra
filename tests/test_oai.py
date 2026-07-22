from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event, Thread
from typing import Any

import httpx
import pytest

import huldra.broker as broker_module
import huldra.db as db_module
import huldra.limiter as limiter_module
from huldra.broker import HuldraBroker
from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.fetcher import FetchResult, NonRetryableFetchError, RateLimitedError, TransientFetchError
from huldra.models import ArxivRequest, OaiHarvestMode, OaiHarvestRequest, OaiMetadataPrefix
from huldra.oai import OaiPmhFetcher, OaiPmhPage, parse_oai_pmh_list_records
from huldra.worker import HuldraWorker
from tests.conftest import make_paper

OAI_PAGE = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-05-28T00:00:00Z</responseDate>
  <ListRecords>
    <record>
      <header>
        <identifier>oai:arXiv.org:2401.00001</identifier>
        <datestamp>2026-05-27</datestamp>
        <setSpec>cs:cs:AI</setSpec>
      </header>
      <metadata>
        <arXiv xmlns="http://arxiv.org/OAI/arXiv/">
          <id>2401.00001</id>
          <created>2024-01-01</created>
          <updated>2024-01-02</updated>
          <authors>
            <author>
              <keyname>Lovelace</keyname>
              <forenames>Ada</forenames>
              <affiliation>Analytical Engine Lab</affiliation>
            </author>
          </authors>
          <title>OAI Paper</title>
          <categories>cs.AI cs.LG</categories>
          <comments>10 pages</comments>
          <journal-ref>Journal</journal-ref>
          <doi>10.1234/oai</doi>
          <license>https://creativecommons.org/licenses/by/4.0/</license>
          <abstract>Abstract text.</abstract>
        </arXiv>
      </metadata>
    </record>
    <resumptionToken>next-token</resumptionToken>
  </ListRecords>
</OAI-PMH>"""

OAI_DELETED_PAGE = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-05-29T00:00:00Z</responseDate>
  <ListRecords>
    <record>
      <header status="deleted">
        <identifier>oai:arXiv.org:2401.00002</identifier>
        <datestamp>2026-05-28</datestamp>
        <setSpec>cs:cs:AI</setSpec>
      </header>
    </record>
  </ListRecords>
</OAI-PMH>"""

OAI_RAW_PAGE = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-05-28T00:00:00Z</responseDate>
  <ListRecords>
    <record>
      <header>
        <identifier>oai:arXiv.org:2401.00003</identifier>
        <datestamp>2026-05-27</datestamp>
        <setSpec>cs:cs:AI</setSpec>
      </header>
      <metadata>
        <arXivRaw xmlns="http://arxiv.org/OAI/arXivRaw/">
          <id>2401.00003</id>
          <authors>Ada Lovelace and Grace Hopper</authors>
          <title>Raw OAI Paper</title>
          <categories>cs.AI cs.LG</categories>
          <comments>12 pages</comments>
          <journal-ref>Raw Journal</journal-ref>
          <doi>10.1234/raw</doi>
          <license>https://creativecommons.org/licenses/by/4.0/</license>
          <abstract>Raw abstract text.</abstract>
          <versions>
            <version>
              <version>v1</version>
              <date>Mon, 01 Jan 2024 00:00:00 GMT</date>
              <size>10kb</size>
              <source_type>I</source_type>
            </version>
            <version>
              <version>v2</version>
              <date>Tue, 02 Jan 2024 00:00:00 GMT</date>
              <size>11kb</size>
              <source_type>I</source_type>
            </version>
          </versions>
        </arXivRaw>
      </metadata>
    </record>
  </ListRecords>
</OAI-PMH>"""

OAI_ERROR = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-05-28T00:00:00Z</responseDate>
  <error code="badArgument">bad from value</error>
</OAI-PMH>"""

OAI_NO_RECORDS_MATCH = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-05-28T00:00:00Z</responseDate>
  <error code="noRecordsMatch">no records found</error>
</OAI-PMH>"""

WELL_FORMED_NON_OAI_BODY = """<?xml version="1.0" encoding="UTF-8"?>
<html>
  <body>temporarily unavailable</body>
</html>"""

OAI_MALFORMED_RECORD_PAGE = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-05-28T00:00:00Z</responseDate>
  <ListRecords>
    <record>
      <metadata>
        <arXiv xmlns="http://arxiv.org/OAI/arXiv/">
          <id>2401.00004</id>
          <created>2024-01-01</created>
          <title>Missing Header</title>
        </arXiv>
      </metadata>
    </record>
  </ListRecords>
</OAI-PMH>"""

OAI_MISSING_METADATA_RECORD_PAGE = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-05-28T00:00:00Z</responseDate>
  <ListRecords>
    <record>
      <header>
        <identifier>oai:arXiv.org:2401.00005</identifier>
        <datestamp>2026-05-27</datestamp>
      </header>
    </record>
  </ListRecords>
</OAI-PMH>"""

OAI_MISSING_IDENTIFIER_RECORD_PAGE = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-05-28T00:00:00Z</responseDate>
  <ListRecords>
    <record>
      <header>
        <datestamp>2026-05-27</datestamp>
      </header>
      <metadata>
        <arXiv xmlns="http://arxiv.org/OAI/arXiv/">
          <id>2401.00006</id>
          <created>2024-01-01</created>
          <title>Missing Identifier</title>
        </arXiv>
      </metadata>
    </record>
  </ListRecords>
</OAI-PMH>"""

OAI_BLANK_IDENTIFIER_RECORD_PAGE = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-05-28T00:00:00Z</responseDate>
  <ListRecords>
    <record>
      <header>
        <identifier>   </identifier>
        <datestamp>2026-05-27</datestamp>
      </header>
      <metadata>
        <arXiv xmlns="http://arxiv.org/OAI/arXiv/">
          <id>2401.00007</id>
          <created>2024-01-01</created>
          <title>Blank Identifier</title>
        </arXiv>
      </metadata>
    </record>
  </ListRecords>
</OAI-PMH>"""

OAI_MISSING_DATESTAMP_RECORD_PAGE = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-05-28T00:00:00Z</responseDate>
  <ListRecords>
    <record>
      <header>
        <identifier>oai:arXiv.org:2401.00008</identifier>
      </header>
      <metadata>
        <arXiv xmlns="http://arxiv.org/OAI/arXiv/">
          <id>2401.00008</id>
          <created>2024-01-01</created>
          <title>Missing Datestamp</title>
        </arXiv>
      </metadata>
    </record>
  </ListRecords>
</OAI-PMH>"""

OAI_INVALID_DATESTAMP_RECORD_PAGE = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-05-28T00:00:00Z</responseDate>
  <ListRecords>
    <record>
      <header status="deleted">
        <identifier>oai:arXiv.org:2401.00009</identifier>
        <datestamp>not-a-date</datestamp>
      </header>
    </record>
  </ListRecords>
</OAI-PMH>"""


@dataclass
class FakeOaiFetcher:
    responses: list[OaiPmhPage | Exception]
    seen: list[dict[str, Any]]

    def list_records(
        self,
        *,
        metadata_prefix: OaiMetadataPrefix,
        set_spec: str | None = None,
        from_datestamp: str | None = None,
        until_datestamp: str | None = None,
        resumption_token: str | None = None,
        timeout_seconds: float | None = None,
    ) -> OaiPmhPage:
        self.seen.append(
            {
                "metadata_prefix": metadata_prefix,
                "set_spec": set_spec,
                "from_datestamp": from_datestamp,
                "until_datestamp": until_datestamp,
                "resumption_token": resumption_token,
                "timeout_seconds": timeout_seconds,
            }
        )
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


@dataclass
class FakeLegacyFetcher:
    responses: list[FetchResult | Exception]
    calls: int = 0

    def fetch(self, request: ArxivRequest) -> FetchResult:
        self.calls += 1
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def test_oai_parser_handles_arxiv_record_deleted_header_error_and_token() -> None:
    page = parse_oai_pmh_list_records(OAI_PAGE)
    deleted = parse_oai_pmh_list_records(OAI_DELETED_PAGE)
    error = parse_oai_pmh_list_records(OAI_ERROR)
    empty = parse_oai_pmh_list_records(OAI_NO_RECORDS_MATCH)

    assert page.response_date == "2026-05-28T00:00:00Z"
    assert page.resumption_token == "next-token"
    assert page.records[0].paper is not None
    assert page.records[0].paper.arxiv_id == "2401.00001"
    assert page.records[0].paper.authors == ["Ada Lovelace"]
    assert page.records[0].paper.authors_detail[0]["affiliation"] == "Analytical Engine Lab"
    assert page.records[0].paper.license == "https://creativecommons.org/licenses/by/4.0/"
    assert deleted.records[0].deleted
    assert deleted.records[0].arxiv_id == "2401.00002"
    assert error.errors[0].code == "badArgument"
    assert empty.records == []
    assert empty.errors[0].code == "noRecordsMatch"


def test_oai_parser_rejects_missing_list_records_without_oai_error() -> None:
    with pytest.raises(ValueError, match="missing ListRecords"):
        parse_oai_pmh_list_records(WELL_FORMED_NON_OAI_BODY)


def test_oai_parser_handles_arxiv_raw_record_versions_and_metadata() -> None:
    page = parse_oai_pmh_list_records(OAI_RAW_PAGE, metadata_prefix="arXivRaw")

    paper = page.records[0].paper
    assert paper is not None
    assert paper.arxiv_id == "2401.00003"
    assert paper.title == "Raw OAI Paper"
    assert paper.authors == ["Ada Lovelace", "Grace Hopper"]
    assert paper.primary_category == "cs.AI"
    assert paper.categories == ["cs.AI", "cs.LG"]
    assert paper.comment == "12 pages"
    assert paper.journal_ref == "Raw Journal"
    assert paper.doi == "10.1234/raw"
    assert paper.license == "https://creativecommons.org/licenses/by/4.0/"
    assert paper.version == 2
    assert [version["version"] for version in paper.versions] == ["v1", "v2"]
    assert paper.updated_at is not None
    assert paper.updated_at.isoformat() == "2024-01-02T00:00:00+00:00"


def test_oai_parser_rejects_non_deleted_record_without_metadata() -> None:
    with pytest.raises(ValueError, match="missing metadata"):
        parse_oai_pmh_list_records(OAI_MISSING_METADATA_RECORD_PAGE)


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(OAI_MISSING_IDENTIFIER_RECORD_PAGE, id="missing"),
        pytest.param(OAI_BLANK_IDENTIFIER_RECORD_PAGE, id="blank"),
    ],
)
def test_oai_parser_rejects_record_missing_identifier(body: str) -> None:
    with pytest.raises(ValueError, match="missing identifier"):
        parse_oai_pmh_list_records(body)


@pytest.mark.parametrize(
    ("body", "expected_message"),
    [
        pytest.param(OAI_MISSING_DATESTAMP_RECORD_PAGE, "missing datestamp", id="missing"),
        pytest.param(OAI_INVALID_DATESTAMP_RECORD_PAGE, "invalid datestamp", id="invalid"),
    ],
)
def test_oai_parser_rejects_record_missing_or_invalid_datestamp(
    body: str,
    expected_message: str,
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        parse_oai_pmh_list_records(body)


def test_oai_fetcher_503_retry_after_enters_rate_limit_flow(settings: HuldraSettings) -> None:
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(503, headers={"Retry-After": "42"})
        )
    )

    with pytest.raises(RateLimitedError) as exc:
        OaiPmhFetcher(settings, client=client).list_records(metadata_prefix="arXiv")

    assert exc.value.retry_after_seconds == 42
    assert exc.value.rate_limit_kind == "oai_503_retry_after"
    assert exc.value.api_family == "oai_pmh"


def test_oai_fetcher_429_stays_a_true_429_rate_limit(settings: HuldraSettings) -> None:
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(429, headers={"Retry-After": "42"})
        )
    )

    with pytest.raises(RateLimitedError) as exc:
        OaiPmhFetcher(settings, client=client).list_records(metadata_prefix="arXiv")

    assert exc.value.status_code == 429
    assert exc.value.retry_after_seconds == 42
    assert exc.value.rate_limit_kind == "http_429"
    assert exc.value.api_family == "oai_pmh"


def test_oai_fetcher_caps_http_timeout_to_remaining_runtime(
    settings: HuldraSettings,
) -> None:
    observed_timeouts: list[dict[str, float]] = []

    def respond(request: httpx.Request) -> httpx.Response:
        observed_timeouts.append(request.extensions["timeout"])
        return httpx.Response(200, text=OAI_NO_RECORDS_MATCH)

    client = httpx.Client(transport=httpx.MockTransport(respond))

    OaiPmhFetcher(settings, client=client).list_records(
        metadata_prefix="arXiv",
        timeout_seconds=0.05,
    )

    assert len(observed_timeouts) == 1
    assert set(observed_timeouts[0].values()) == {0.05}


def test_oai_fetcher_malformed_200_raises_transient_fetch_error(
    settings: HuldraSettings,
) -> None:
    client = httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, text="<OAI-PMH"))
    )

    with pytest.raises(TransientFetchError) as exc:
        OaiPmhFetcher(settings, client=client).list_records(metadata_prefix="arXiv")

    assert exc.value.status_code == 200
    assert "malformed XML" in str(exc.value)


def test_oai_fetcher_well_formed_non_oai_200_raises_transient_fetch_error(
    settings: HuldraSettings,
) -> None:
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, text=WELL_FORMED_NON_OAI_BODY)
        )
    )

    with pytest.raises(TransientFetchError) as exc:
        OaiPmhFetcher(settings, client=client).list_records(metadata_prefix="arXiv")

    assert exc.value.status_code == 200
    assert "missing ListRecords" in str(exc.value)


@pytest.mark.parametrize(
    ("body", "expected_message"),
    [
        pytest.param(OAI_MALFORMED_RECORD_PAGE, "missing header", id="missing-header"),
        pytest.param(OAI_MISSING_METADATA_RECORD_PAGE, "missing metadata", id="missing-metadata"),
        pytest.param(OAI_MISSING_IDENTIFIER_RECORD_PAGE, "missing identifier", id="missing-identifier"),
        pytest.param(OAI_BLANK_IDENTIFIER_RECORD_PAGE, "missing identifier", id="blank-identifier"),
        pytest.param(OAI_MISSING_DATESTAMP_RECORD_PAGE, "missing datestamp", id="missing-datestamp"),
        pytest.param(OAI_INVALID_DATESTAMP_RECORD_PAGE, "invalid datestamp", id="invalid-datestamp"),
    ],
)
def test_oai_fetcher_malformed_record_raises_transient_fetch_error(
    settings: HuldraSettings,
    body: str,
    expected_message: str,
) -> None:
    client = httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, text=body))
    )

    with pytest.raises(TransientFetchError) as exc:
        OaiPmhFetcher(settings, client=client).list_records(metadata_prefix="arXiv")

    assert exc.value.status_code == 200
    assert "malformed OAI record" in str(exc.value)
    assert expected_message in str(exc.value)


def test_oai_harvest_503_retry_after_persists_shared_cooldown(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(503, headers={"Retry-After": "42"})
        )
    )
    broker = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=OaiPmhFetcher(settings, client=client),
    )

    result = broker.harvest_oai(
        OaiHarvestRequest(client_id="test", metadata_prefix="arXiv", mode=OaiHarvestMode.INITIAL)
    )

    rate_state = store.get_rate_state()
    assert result.status == "rate_limited"
    assert result.error_message is not None
    assert "cooldown_until=" in result.error_message
    assert rate_state.cooldown_until is not None
    assert rate_state.last_status == 503
    status = store.status_summary()
    assert status.upstream_429_total == 0
    assert status.upstream_rate_limited_total == 1
    assert status.upstream_oai_503_retry_after_total == 1


def test_oai_harvest_delay_is_shared_with_legacy_worker(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    page = parse_oai_pmh_list_records(
        OAI_PAGE.replace("<resumptionToken>next-token</resumptionToken>", "")
    )
    harvest = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=FakeOaiFetcher([page], []),
    ).harvest_oai(
        OaiHarvestRequest(client_id="test", metadata_prefix="arXiv", mode=OaiHarvestMode.INITIAL)
    )
    store.enqueue_request(ArxivRequest(client_id="legacy", search_query="cat:cs.AI"))
    sleeps: list[float] = []
    fetcher = FakeLegacyFetcher(
        [FetchResult([make_paper("2401.00005v1")], total_results=1)]
    )

    worker = HuldraWorker(store, settings, fetcher=fetcher, sleep=sleeps.append)
    worker_result = worker.run_once()

    assert harvest.status == "completed"
    assert worker_result.status == "completed"
    assert fetcher.calls == 1
    assert sleeps
    assert sleeps[0] > 0


def test_oai_retry_after_cooldown_is_shared_with_legacy_worker(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    harvest = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=FakeOaiFetcher([RateLimitedError(30)], []),
    ).harvest_oai(
        OaiHarvestRequest(client_id="test", metadata_prefix="arXiv", mode=OaiHarvestMode.INITIAL)
    )
    shared_state = store.get_rate_state()
    store.enqueue_request(ArxivRequest(client_id="legacy", search_query="cat:cs.AI"))
    fetcher = FakeLegacyFetcher(
        [FetchResult([make_paper("2401.00006v1")], total_results=1)]
    )

    worker_result = HuldraWorker(
        store,
        settings,
        fetcher=fetcher,
        sleep=lambda _: None,
    ).run_once()

    assert harvest.status == "rate_limited"
    assert shared_state.cooldown_until is not None
    assert store.status_summary().cooldown_active
    assert worker_result.status == "cooling_down"
    assert worker_result.error_category == "cooldown"
    assert worker_result.cooldown_until == shared_state.cooldown_until
    assert fetcher.calls == 0


def test_oai_harvest_malformed_200_records_failure_and_releases_limiter(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    client = httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, text="<OAI-PMH"))
    )
    broker = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=OaiPmhFetcher(settings, client=client),
    )

    result = broker.harvest_oai(
        OaiHarvestRequest(client_id="test", metadata_prefix="arXiv", mode=OaiHarvestMode.INITIAL)
    )

    with store.connect() as conn:
        page = conn.execute("SELECT status, error_category FROM oai_pages").fetchone()
        job = conn.execute("SELECT status, error_category FROM oai_harvest_jobs").fetchone()
    assert result.status == "transient_failure"
    assert result.error_category == "transient"
    assert page is not None
    assert page["status"] == "transient_failure"
    assert page["error_category"] == "transient"
    assert job is not None
    assert job["status"] == "transient_failure"
    assert job["error_category"] == "transient"
    assert store.acquire_lease("upstream_fetch", "probe", 60)


def test_oai_harvest_well_formed_non_oai_200_records_failure_and_preserves_watermark(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    store.set_oai_watermark(
        metadata_prefix="arXiv",
        set_spec=None,
        last_response_date="2026-05-28T00:00:00Z",
        last_datestamp_seen="2026-05-27T00:00:00+00:00",
        harvest_id="previous",
    )
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, text=WELL_FORMED_NON_OAI_BODY)
        )
    )
    broker = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=OaiPmhFetcher(settings, client=client),
    )

    result = broker.harvest_oai(
        OaiHarvestRequest(client_id="test", metadata_prefix="arXiv", mode=OaiHarvestMode.INCREMENTAL)
    )

    watermark = store.get_oai_watermark(metadata_prefix="arXiv", set_spec=None)
    with store.connect() as conn:
        page = conn.execute("SELECT status, error_category FROM oai_pages").fetchone()
        job = conn.execute("SELECT status, error_category FROM oai_harvest_jobs").fetchone()
    assert result.status == "transient_failure"
    assert result.error_category == "transient"
    assert page is not None
    assert page["status"] == "transient_failure"
    assert page["error_category"] == "transient"
    assert job is not None
    assert job["status"] == "transient_failure"
    assert job["error_category"] == "transient"
    assert watermark is not None
    assert watermark["last_response_date"] == "2026-05-28T00:00:00Z"
    assert watermark["last_datestamp_seen"] == "2026-05-27T00:00:00+00:00"
    assert watermark["last_successful_harvest_id"] == "previous"


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(OAI_MALFORMED_RECORD_PAGE, id="missing-header"),
        pytest.param(OAI_MISSING_METADATA_RECORD_PAGE, id="missing-metadata"),
        pytest.param(OAI_MISSING_IDENTIFIER_RECORD_PAGE, id="missing-identifier"),
        pytest.param(OAI_BLANK_IDENTIFIER_RECORD_PAGE, id="blank-identifier"),
        pytest.param(OAI_MISSING_DATESTAMP_RECORD_PAGE, id="missing-datestamp"),
        pytest.param(OAI_INVALID_DATESTAMP_RECORD_PAGE, id="invalid-datestamp"),
    ],
)
def test_oai_harvest_malformed_record_records_failure_and_releases_limiter(
    store: HuldraStore,
    settings: HuldraSettings,
    body: str,
) -> None:
    client = httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, text=body))
    )
    broker = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=OaiPmhFetcher(settings, client=client),
    )

    result = broker.harvest_oai(
        OaiHarvestRequest(client_id="test", metadata_prefix="arXiv", mode=OaiHarvestMode.INITIAL)
    )

    with store.connect() as conn:
        page = conn.execute("SELECT status, error_category FROM oai_pages").fetchone()
        job = conn.execute("SELECT status, error_category FROM oai_harvest_jobs").fetchone()
    assert result.status == "transient_failure"
    assert result.error_category == "transient"
    assert page is not None
    assert page["status"] == "transient_failure"
    assert page["error_category"] == "transient"
    assert job is not None
    assert job["status"] == "transient_failure"
    assert job["error_category"] == "transient"
    assert store.acquire_lease("upstream_fetch", "probe", 60)


def test_oai_harvest_follows_resumption_token_and_advances_watermark(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    first = parse_oai_pmh_list_records(OAI_PAGE)
    second = parse_oai_pmh_list_records(OAI_DELETED_PAGE)
    fetcher = FakeOaiFetcher([first, second], [])

    result = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=fetcher,
    ).harvest_oai(
        OaiHarvestRequest(client_id="test", metadata_prefix="arXiv", mode=OaiHarvestMode.INITIAL)
    )

    assert result.status == "completed"
    assert result.records_processed == 2
    assert result.papers_upserted == 1
    assert result.deleted_records == 1
    assert result.pages_total == 2
    assert result.current_watermark == "2026-05-29"
    assert fetcher.seen[1]["resumption_token"] == "next-token"
    assert store.get_paper("2401.00001") is not None
    watermark = store.get_oai_watermark(metadata_prefix="arXiv", set_spec=None)
    assert watermark is not None
    assert watermark["last_response_date"] == "2026-05-29"
    assert watermark["last_datestamp_seen"] == "2026-05-28"


def test_oai_failed_page_does_not_advance_watermark(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    first = parse_oai_pmh_list_records(OAI_PAGE)
    fetcher = FakeOaiFetcher([first, NonRetryableFetchError("bad token", status_code=200)], [])

    result = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=fetcher,
    ).harvest_oai(
        OaiHarvestRequest(client_id="test", metadata_prefix="arXiv", mode=OaiHarvestMode.INITIAL)
    )

    assert result.status == "failed"
    assert result.records_processed == 1
    assert result.pages_total == 2
    assert result.resumption_token == "next-token"
    assert store.get_oai_watermark(metadata_prefix="arXiv", set_spec=None) is None


def test_oai_harvest_auto_resumes_pending_token_after_rate_limit(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    first = parse_oai_pmh_list_records(OAI_PAGE)
    final = parse_oai_pmh_list_records(OAI_DELETED_PAGE)
    interrupted_fetcher = FakeOaiFetcher([first, RateLimitedError(0)], [])

    interrupted = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=interrupted_fetcher,
    ).harvest_oai(
        OaiHarvestRequest(client_id="test", metadata_prefix="arXiv", mode=OaiHarvestMode.INITIAL)
    )

    assert interrupted.status == "rate_limited"
    assert interrupted.records_processed == 1
    assert interrupted.resumption_token == "next-token"
    assert interrupted_fetcher.seen[1]["resumption_token"] == "next-token"

    rate = store.get_rate_state()
    store.set_rate_state(rate.model_copy(update={"cooldown_until": None}))

    resume_fetcher = FakeOaiFetcher([final], [])
    resumed = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=resume_fetcher,
    ).harvest_oai(
        OaiHarvestRequest(client_id="test", metadata_prefix="arXiv", mode=OaiHarvestMode.INITIAL)
    )

    assert resumed.status == "completed"
    assert resumed.records_processed == 1
    assert resumed.current_watermark == "2026-05-29"
    assert resume_fetcher.seen[0]["resumption_token"] == "next-token"
    assert resume_fetcher.seen[0]["from_datestamp"] is None
    watermark = store.get_oai_watermark(metadata_prefix="arXiv", set_spec=None)
    assert watermark is not None
    assert watermark["last_response_date"] == "2026-05-29"

    fresh_fetcher = FakeOaiFetcher([final], [])
    fresh = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=fresh_fetcher,
    ).harvest_oai(
        OaiHarvestRequest(client_id="test", metadata_prefix="arXiv", mode=OaiHarvestMode.INITIAL)
    )

    assert fresh.status == "completed"
    assert fresh_fetcher.seen[0]["resumption_token"] is None


def test_oai_running_harvest_resumes_from_atomic_page_checkpoint(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    first = parse_oai_pmh_list_records(OAI_PAGE)
    final = parse_oai_pmh_list_records(OAI_DELETED_PAGE)
    crashed_fetcher = FakeOaiFetcher([first, RuntimeError("simulated process crash")], [])
    request = OaiHarvestRequest(
        client_id="test",
        metadata_prefix="arXiv",
        mode=OaiHarvestMode.INITIAL,
        max_pages=10,
        max_requests=10,
        runtime_budget_seconds=60,
    )

    with pytest.raises(RuntimeError, match="simulated process crash"):
        HuldraBroker(
            store=store,
            settings=settings,
            oai_fetcher=crashed_fetcher,
        ).harvest_oai(request)

    with store.begin_immediate() as conn:
        conn.execute("DELETE FROM leases")
        checkpoint = conn.execute(
            "SELECT harvest_id, status, pages_total, resumption_token FROM oai_harvest_jobs"
        ).fetchone()
    assert checkpoint is not None
    assert checkpoint["status"] == "running"
    assert checkpoint["pages_total"] == 1
    assert checkpoint["resumption_token"] == "next-token"

    resumed_fetcher = FakeOaiFetcher([final], [])
    resumed = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=resumed_fetcher,
    ).harvest_oai(request)

    assert resumed.harvest_id == checkpoint["harvest_id"]
    assert resumed.status == "completed"
    assert resumed.pages_total == 2
    assert resumed.records_processed == 2
    assert resumed_fetcher.seen[0]["resumption_token"] == "next-token"
    with store.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM oai_harvest_jobs").fetchone()[0] == 1


def test_oai_explicit_token_does_not_recover_running_job_at_other_cursor(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    crashed_request = OaiHarvestRequest(
        client_id="crashed",
        metadata_prefix="arXiv",
        mode=OaiHarvestMode.INITIAL,
    )
    crashed_harvest_id = store.create_oai_harvest_job(
        crashed_request,
        resumption_token="crashed-token",
    )
    fetcher = FakeOaiFetcher(
        [parse_oai_pmh_list_records(OAI_DELETED_PAGE)],
        [],
    )

    result = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=fetcher,
    ).harvest_oai(
        OaiHarvestRequest(
            client_id="manual",
            metadata_prefix="arXiv",
            mode=OaiHarvestMode.INITIAL,
            resumption_token="caller-token",
        )
    )

    assert result.status == "completed"
    assert result.harvest_id != crashed_harvest_id
    assert fetcher.seen[0]["resumption_token"] == "caller-token"


def test_oai_repeated_resumption_token_fails_without_refetch_loop(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    page = replace(parse_oai_pmh_list_records(OAI_PAGE), resumption_token="loop-token")
    fetcher = FakeOaiFetcher([page], [])

    result = HuldraBroker(store=store, settings=settings, oai_fetcher=fetcher).harvest_oai(
        OaiHarvestRequest(
            client_id="test",
            metadata_prefix="arXiv",
            mode=OaiHarvestMode.INITIAL,
            resumption_token="loop-token",
        )
    )

    assert result.status == "failed"
    assert result.error_category == "repeated_resumption_token"
    assert len(fetcher.seen) == 1


def test_oai_invalid_token_checkpoint_crash_does_not_refetch_invalid_cursor(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(broker_module.time, "sleep", lambda _seconds: None)
    invalid = OaiPmhPage(
        records=[],
        response_date="2026-07-22T00:00:00Z",
        resumption_token="loop-token",
        errors=[],
        request_params={},
    )
    request = OaiHarvestRequest(
        client_id="test",
        metadata_prefix="arXiv",
        mode=OaiHarvestMode.INITIAL,
        resumption_token="loop-token",
        max_pages=10,
        max_requests=10,
    )
    original_checkpoint = store.checkpoint_oai_page

    def checkpoint_then_crash(*args: Any, **kwargs: Any) -> tuple[int, int, int]:
        original_checkpoint(*args, **kwargs)
        store.checkpoint_oai_page = original_checkpoint  # type: ignore[method-assign]
        raise RuntimeError("simulated crash after invalid cursor checkpoint")

    store.checkpoint_oai_page = checkpoint_then_crash  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="simulated crash"):
        HuldraBroker(
            store=store,
            settings=settings,
            oai_fetcher=FakeOaiFetcher([invalid], []),
        ).harvest_oai(request)

    with store.connect() as conn:
        checkpoint = conn.execute(
            "SELECT status, error_category, resumption_token FROM oai_harvest_jobs"
        ).fetchone()
    assert checkpoint is not None
    assert checkpoint["status"] == "running"
    assert checkpoint["error_category"] == "repeated_resumption_token"
    assert checkpoint["resumption_token"] == "loop-token"

    resumed_fetcher = FakeOaiFetcher([], [])
    resumed = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=resumed_fetcher,
    ).harvest_oai(request)

    assert resumed.status == "failed"
    assert resumed.error_category == "repeated_resumption_token"
    assert resumed_fetcher.seen == []


def test_oai_resumption_token_cycle_is_bounded(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    first = replace(parse_oai_pmh_list_records(OAI_PAGE), resumption_token="token-a")
    second = replace(parse_oai_pmh_list_records(OAI_DELETED_PAGE), resumption_token="token-b")
    third = replace(parse_oai_pmh_list_records(OAI_DELETED_PAGE), resumption_token="token-a")
    fetcher = FakeOaiFetcher([first, second, third], [])

    result = HuldraBroker(store=store, settings=settings, oai_fetcher=fetcher).harvest_oai(
        OaiHarvestRequest(client_id="test", metadata_prefix="arXiv", mode=OaiHarvestMode.INITIAL)
    )

    assert result.status == "failed"
    assert result.error_category == "resumption_token_cycle"
    assert len(fetcher.seen) == 3


def test_oai_empty_page_with_continuation_token_fails_as_no_progress(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    page = OaiPmhPage(
        records=[],
        response_date="2026-05-28T00:00:00Z",
        resumption_token="next-token",
        errors=[],
        request_params={},
    )
    fetcher = FakeOaiFetcher([page], [])

    result = HuldraBroker(store=store, settings=settings, oai_fetcher=fetcher).harvest_oai(
        OaiHarvestRequest(client_id="test", metadata_prefix="arXiv", mode=OaiHarvestMode.INITIAL)
    )

    assert result.status == "failed"
    assert result.error_category == "no_progress"
    assert len(fetcher.seen) == 1


def test_oai_request_budget_stops_before_next_fetch(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    first = parse_oai_pmh_list_records(OAI_PAGE)
    fetcher = FakeOaiFetcher([first], [])

    result = HuldraBroker(store=store, settings=settings, oai_fetcher=fetcher).harvest_oai(
        OaiHarvestRequest(
            client_id="test",
            metadata_prefix="arXiv",
            mode=OaiHarvestMode.INITIAL,
            max_pages=10,
            max_requests=1,
            runtime_budget_seconds=60,
        )
    )

    assert result.status == "budget_exceeded"
    assert result.error_category == "request_budget_exceeded"
    assert result.pages_total == 1
    assert result.requests_total == 1
    assert len(fetcher.seen) == 1


def test_oai_expired_runtime_budget_stops_before_fetch(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = OaiHarvestRequest(
        client_id="test",
        metadata_prefix="arXiv",
        mode=OaiHarvestMode.INITIAL,
        runtime_budget_seconds=60,
    )
    harvest_id = store.create_oai_harvest_job(request)
    with store.begin_immediate() as conn:
        conn.execute(
            "UPDATE oai_harvest_jobs SET deadline_at='2000-01-01T00:00:00+00:00' "
            "WHERE harvest_id=?",
            (harvest_id,),
        )
    fetcher = FakeOaiFetcher([], [])

    result = HuldraBroker(store=store, settings=settings, oai_fetcher=fetcher).harvest_oai(request)

    assert result.harvest_id == harvest_id
    assert result.status == "budget_exceeded"
    assert result.error_category == "runtime_budget_exceeded"
    assert fetcher.seen == []


def test_oai_harvest_caps_each_request_to_remaining_runtime(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
    clock = _OaiAuditClock(started)
    monkeypatch.setattr(broker_module, "utc_now", clock.now)
    monkeypatch.setattr(db_module, "utc_now", clock.now)
    monkeypatch.setattr(limiter_module, "utc_now", clock.now)
    original_started = store.record_oai_request_started
    original_renew = store.renew_leases_if_owned

    def account_with_delay(harvest_id: str) -> int:
        requests_total = original_started(harvest_id)
        clock.current += timedelta(seconds=1)
        return requests_total

    def renew_with_delay(
        leases: tuple[tuple[str, str, int], ...],
        *,
        now: datetime | None = None,
    ) -> bool:
        renewed = original_renew(leases, now=now)
        if len(leases) == 2:
            clock.current += timedelta(seconds=2)
        return renewed

    monkeypatch.setattr(store, "record_oai_request_started", account_with_delay)
    monkeypatch.setattr(store, "renew_leases_if_owned", renew_with_delay)
    fetcher = FakeOaiFetcher([parse_oai_pmh_list_records(OAI_DELETED_PAGE)], [])
    tuned = settings.model_copy(update={"request_timeout_seconds": 30.0})

    result = HuldraBroker(
        store=store,
        settings=tuned,
        oai_fetcher=fetcher,
    ).harvest_oai(
        OaiHarvestRequest(
            client_id="test",
            metadata_prefix="arXiv",
            mode=OaiHarvestMode.INITIAL,
            runtime_budget_seconds=4,
            max_pages=1,
            max_requests=1,
        )
    )

    assert result.status == "completed"
    assert fetcher.seen[0]["timeout_seconds"] == 1.0


def test_oai_running_harvest_backfills_missing_deadline_once(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    request = OaiHarvestRequest(
        client_id="test",
        metadata_prefix="arXiv",
        mode=OaiHarvestMode.INITIAL,
        runtime_budget_seconds=60,
    )
    harvest_id = store.create_oai_harvest_job(request)
    with store.begin_immediate() as conn:
        conn.execute(
            "UPDATE oai_harvest_jobs SET deadline_at=NULL WHERE harvest_id=?",
            (harvest_id,),
        )
    fetcher = FakeOaiFetcher([parse_oai_pmh_list_records(OAI_DELETED_PAGE)], [])

    result = HuldraBroker(store=store, settings=settings, oai_fetcher=fetcher).harvest_oai(request)

    assert result.harvest_id == harvest_id
    assert result.status == "completed"
    assert result.deadline_at is not None
    with store.connect() as conn:
        deadline = conn.execute(
            "SELECT deadline_at FROM oai_harvest_jobs WHERE harvest_id=?",
            (harvest_id,),
        ).fetchone()[0]
    assert deadline == result.deadline_at.isoformat()


@dataclass
class _OaiAuditClock:
    current: datetime

    def now(self) -> datetime:
        return self.current

    def oversleep(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds + 2)


@dataclass
class _TimedEmptyOaiFetcher:
    clock: _OaiAuditClock
    calls_at: list[datetime]

    def list_records(self, **_kwargs: object) -> OaiPmhPage:
        self.calls_at.append(self.clock.now())
        return OaiPmhPage(
            records=[],
            response_date="2026-07-22T00:00:00Z",
            resumption_token=None,
            errors=[],
            request_params={},
        )


def test_oai_rechecks_deadline_after_rate_wait_before_network(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
    clock = _OaiAuditClock(started)
    monkeypatch.setattr(broker_module, "utc_now", clock.now)
    monkeypatch.setattr(db_module, "utc_now", clock.now)
    monkeypatch.setattr(limiter_module, "utc_now", clock.now)
    monkeypatch.setattr(broker_module.time, "sleep", clock.oversleep)
    store.set_rate_state(store.get_rate_state().model_copy(update={"last_request_at": started}))
    fetcher = FakeOaiFetcher([], [])

    result = HuldraBroker(store=store, settings=settings, oai_fetcher=fetcher).harvest_oai(
        OaiHarvestRequest(
            client_id="test",
            metadata_prefix="arXiv",
            mode=OaiHarvestMode.INITIAL,
            runtime_budget_seconds=4,
            max_pages=1,
            max_requests=1,
        )
    )

    assert result.deadline_at == started + timedelta(seconds=4)
    assert result.status == "budget_exceeded"
    assert result.error_category == "runtime_budget_exceeded"
    assert fetcher.seen == []


def test_oai_accounting_db_wait_cannot_expire_scope_and_upstream_leases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
    clock = _OaiAuditClock(started)
    tuned = HuldraSettings(
        db_path=tmp_path / "oai-accounting-db-wait.db",
        request_interval_seconds=3.0,
        cooldown_seconds=60,
        rate_limit_jitter_seconds=0.0,
        worker_poll_interval_seconds=1.0,
        request_timeout_seconds=0.2,
        lease_timeout_seconds=3,
    )
    monkeypatch.setattr(broker_module, "utc_now", clock.now)
    monkeypatch.setattr(db_module, "utc_now", clock.now)
    monkeypatch.setattr(limiter_module, "utc_now", clock.now)

    seed = HuldraStore(tuned.db_path)
    seed.init_schema()
    first_store = HuldraStore(tuned.db_path, timeout=5)
    second_store = HuldraStore(tuned.db_path, timeout=5)
    fetcher = _TimedEmptyOaiFetcher(clock, [])
    request = OaiHarvestRequest(
        client_id="test",
        mode=OaiHarvestMode.INITIAL,
        runtime_budget_seconds=30,
        max_pages=1,
        max_requests=2,
    )

    accounting_stage = Event()
    blocker_ready = Event()
    lock_attempted = Event()
    accounting_committed = Event()
    allow_first_to_continue = Event()
    accounting_active = Event()
    errors: list[BaseException] = []
    first_results: list[object] = []
    original_begin = first_store.begin_immediate
    original_account = first_store.record_oai_request_started

    @contextmanager
    def signal_accounting_lock_attempt() -> Iterator[sqlite3.Connection]:
        if accounting_active.is_set():
            lock_attempted.set()
        with original_begin() as conn:
            yield conn

    def blocked_account(harvest_id: str) -> int:
        accounting_stage.set()
        assert blocker_ready.wait(timeout=5)
        accounting_active.set()
        try:
            count = original_account(harvest_id)
        finally:
            accounting_active.clear()
        accounting_committed.set()
        assert allow_first_to_continue.wait(timeout=5)
        return count

    monkeypatch.setattr(first_store, "begin_immediate", signal_accounting_lock_attempt)
    monkeypatch.setattr(first_store, "record_oai_request_started", blocked_account)

    first_broker = HuldraBroker(first_store, tuned, oai_fetcher=fetcher)
    second_broker = HuldraBroker(second_store, tuned, oai_fetcher=fetcher)

    def run_first() -> None:
        try:
            first_results.append(first_broker.harvest_oai(request))
        except BaseException as exc:
            errors.append(exc)

    first_thread = Thread(target=run_first, daemon=True)
    first_thread.start()
    assert accounting_stage.wait(timeout=2)

    blocker = sqlite3.connect(tuned.db_path, timeout=5)
    blocker.execute("BEGIN IMMEDIATE")
    blocker_ready.set()
    assert lock_attempted.wait(timeout=2)
    # Request accounting waits past both the scope and upstream lease deadlines.
    clock.current += timedelta(seconds=15)
    blocker.commit()
    blocker.close()
    assert accounting_committed.wait(timeout=2)

    second_result = second_broker.harvest_oai(request)
    allow_first_to_continue.set()
    first_thread.join(timeout=5)

    assert not first_thread.is_alive()
    assert second_result.status in {"completed", "blocked", "budget_exceeded"}
    assert len(fetcher.calls_at) <= 1
    assert not errors


def test_oai_scope_lease_covers_sqlite_wait_and_post_fetch_persistence(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tuned = settings.model_copy(update={"lease_timeout_seconds": 1})
    observed_scope_timeouts: list[int] = []
    acquire = store.acquire_lease

    def capture_scope_timeout(
        name: str,
        owner_token: str,
        timeout_seconds: int,
        *,
        now: datetime | None = None,
    ) -> bool:
        if name != "upstream_fetch":
            observed_scope_timeouts.append(timeout_seconds)
        return acquire(name, owner_token, timeout_seconds, now=now)

    monkeypatch.setattr(store, "acquire_lease", capture_scope_timeout)
    page = OaiPmhPage(
        records=[],
        response_date="2026-07-22T00:00:00Z",
        resumption_token=None,
        errors=[],
        request_params={},
    )

    result = HuldraBroker(
        store,
        tuned,
        oai_fetcher=FakeOaiFetcher([page], []),
    ).harvest_oai(
        OaiHarvestRequest(
            client_id="test",
            mode=OaiHarvestMode.INITIAL,
            max_pages=1,
            max_requests=1,
        )
    )

    assert result.status == "completed"
    assert observed_scope_timeouts == [44]


def test_oai_rechecks_deadline_after_request_accounting_before_network(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
    clock = _OaiAuditClock(started)
    monkeypatch.setattr(broker_module, "utc_now", clock.now)
    monkeypatch.setattr(db_module, "utc_now", clock.now)
    monkeypatch.setattr(limiter_module, "utc_now", clock.now)
    original_started = store.record_oai_request_started

    def account_then_expire(harvest_id: str) -> int:
        requests_total = original_started(harvest_id)
        clock.current += timedelta(seconds=5)
        return requests_total

    monkeypatch.setattr(store, "record_oai_request_started", account_then_expire)
    fetcher = FakeOaiFetcher([], [])

    result = HuldraBroker(store=store, settings=settings, oai_fetcher=fetcher).harvest_oai(
        OaiHarvestRequest(
            client_id="test",
            metadata_prefix="arXiv",
            mode=OaiHarvestMode.INITIAL,
            runtime_budget_seconds=4,
            max_pages=1,
            max_requests=1,
        )
    )

    assert result.status == "budget_exceeded"
    assert result.error_category == "runtime_budget_exceeded"
    assert result.requests_total == 1
    assert fetcher.seen == []


def test_oai_uses_persisted_deadline_as_runtime_source_of_truth(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persisted_now = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
    execution_now = persisted_now + timedelta(seconds=5)
    monkeypatch.setattr(db_module, "utc_now", lambda: persisted_now)
    monkeypatch.setattr(broker_module, "utc_now", lambda: execution_now)
    monkeypatch.setattr(limiter_module, "utc_now", lambda: execution_now)
    fetcher = FakeOaiFetcher([], [])

    result = HuldraBroker(store=store, settings=settings, oai_fetcher=fetcher).harvest_oai(
        OaiHarvestRequest(
            client_id="test",
            metadata_prefix="arXiv",
            mode=OaiHarvestMode.INITIAL,
            runtime_budget_seconds=4,
            max_pages=1,
            max_requests=1,
        )
    )

    assert result.deadline_at == persisted_now + timedelta(seconds=4)
    assert result.status == "budget_exceeded"
    assert fetcher.seen == []


def test_oai_final_page_checkpoint_completes_after_crash_without_refetch(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    final = parse_oai_pmh_list_records(OAI_DELETED_PAGE)
    request = OaiHarvestRequest(
        client_id="test",
        metadata_prefix="arXiv",
        mode=OaiHarvestMode.INITIAL,
    )
    broker = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=FakeOaiFetcher([final], []),
    )
    original = store.set_oai_watermark

    def crash_after_checkpoint(**kwargs: Any) -> None:
        store.set_oai_watermark = original  # type: ignore[method-assign]
        raise RuntimeError("simulated finalization crash")

    store.set_oai_watermark = crash_after_checkpoint  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="simulated finalization crash"):
        broker.harvest_oai(request)

    resumed_fetcher = FakeOaiFetcher([], [])
    result = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=resumed_fetcher,
    ).harvest_oai(request)

    assert result.status == "completed"
    assert result.pages_total == 1
    assert result.records_processed == 1
    assert resumed_fetcher.seen == []


def test_oai_same_scope_harvest_is_single_flight_across_brokers(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    entered = Event()
    release = Event()
    final = parse_oai_pmh_list_records(OAI_DELETED_PAGE)

    class BlockingFetcher:
        calls = 0

        def list_records(self, **_kwargs: Any) -> OaiPmhPage:
            self.calls += 1
            entered.set()
            assert release.wait(timeout=2)
            return final

    fetcher = BlockingFetcher()
    request = OaiHarvestRequest(
        client_id="test",
        metadata_prefix="arXiv",
        mode=OaiHarvestMode.INITIAL,
    )
    first_results: list[object] = []

    def run_first() -> None:
        try:
            first_results.append(
                HuldraBroker(store=store, settings=settings, oai_fetcher=fetcher).harvest_oai(
                    request
                )
            )
        except BaseException as exc:  # pragma: no cover - re-raised below
            first_results.append(exc)

    thread = Thread(target=run_first)
    thread.start()
    assert entered.wait(timeout=2)

    blocked_fetcher = FakeOaiFetcher([], [])
    blocked = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=blocked_fetcher,
    ).harvest_oai(request)

    assert blocked.status == "blocked"
    assert blocked.error_category == "harvest_in_progress"
    assert blocked_fetcher.seen == []
    release.set()
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert len(first_results) == 1
    assert not isinstance(first_results[0], BaseException), first_results
    assert fetcher.calls == 1


def test_oai_initial_and_incremental_watermark_writers_share_scope(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(broker_module.time, "sleep", lambda _seconds: None)
    entered = Event()
    release = Event()
    older = replace(
        parse_oai_pmh_list_records(OAI_DELETED_PAGE),
        response_date="2026-07-20T00:00:00Z",
    )
    newer = replace(
        parse_oai_pmh_list_records(OAI_DELETED_PAGE),
        response_date="2026-07-22T00:00:00Z",
    )

    class BlockingFetcher:
        def list_records(self, **_kwargs: Any) -> OaiPmhPage:
            entered.set()
            assert release.wait(timeout=2)
            return older

    first_results: list[object] = []

    def run_initial() -> None:
        try:
            first_results.append(
                HuldraBroker(
                    store=store,
                    settings=settings,
                    oai_fetcher=BlockingFetcher(),
                ).harvest_oai(
                    OaiHarvestRequest(
                        client_id="initial",
                        metadata_prefix="arXiv",
                        mode=OaiHarvestMode.INITIAL,
                    )
                )
            )
        except BaseException as exc:  # pragma: no cover - re-raised below
            first_results.append(exc)

    thread = Thread(target=run_initial)
    thread.start()
    assert entered.wait(timeout=2)
    second_store = HuldraStore(settings.db_path)
    blocked_fetcher = FakeOaiFetcher([], [])
    incremental_request = OaiHarvestRequest(
        client_id="incremental",
        metadata_prefix="arXiv",
        mode=OaiHarvestMode.INCREMENTAL,
    )

    blocked = HuldraBroker(
        store=second_store,
        settings=settings,
        oai_fetcher=blocked_fetcher,
    ).harvest_oai(incremental_request)

    assert blocked.status == "blocked"
    assert blocked.error_category == "harvest_in_progress"
    assert blocked_fetcher.seen == []
    release.set()
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert len(first_results) == 1
    assert not isinstance(first_results[0], BaseException), first_results

    completed = HuldraBroker(
        store=second_store,
        settings=settings,
        oai_fetcher=FakeOaiFetcher([newer], []),
    ).harvest_oai(incremental_request)

    assert completed.status == "completed"
    watermark = store.get_oai_watermark(metadata_prefix="arXiv", set_spec=None)
    assert watermark is not None
    assert watermark["last_response_date"] == "2026-07-22"


def test_oai_watermark_updates_never_regress(
    store: HuldraStore,
) -> None:
    store.set_oai_watermark(
        metadata_prefix="arXiv",
        set_spec=None,
        last_response_date="2026-07-22",
        last_datestamp_seen="2026-07-21",
        harvest_id="newer",
    )
    store.set_oai_watermark(
        metadata_prefix="arXiv",
        set_spec=None,
        last_response_date="2026-07-20",
        last_datestamp_seen="2026-07-19",
        harvest_id="older",
    )

    watermark = store.get_oai_watermark(metadata_prefix="arXiv", set_spec=None)
    assert watermark is not None
    assert watermark["last_response_date"] == "2026-07-22"
    assert watermark["last_datestamp_seen"] == "2026-07-21"


def test_oai_harvest_request_resumption_token_starts_with_token(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    page = parse_oai_pmh_list_records(
        OAI_DELETED_PAGE.replace(
            "<datestamp>2026-05-28</datestamp>",
            "<datestamp>2026-05-30</datestamp>",
        )
    )
    fetcher = FakeOaiFetcher([page], [])

    result = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=fetcher,
    ).harvest_oai(
        OaiHarvestRequest(
            client_id="test",
            metadata_prefix="arXiv",
            mode=OaiHarvestMode.INITIAL,
            resumption_token="resume-token",
        )
    )

    assert result.status == "completed"
    assert fetcher.seen[0]["resumption_token"] == "resume-token"
    assert fetcher.seen[0]["from_datestamp"] is None


def test_oai_incremental_harvest_resumes_from_successful_watermark(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    store.set_oai_watermark(
        metadata_prefix="arXiv",
        set_spec="cs:cs:AI",
        last_response_date="2026-05-28T00:00:00Z",
        last_datestamp_seen="2026-05-27T00:00:00+00:00",
        harvest_id="previous",
    )
    page = parse_oai_pmh_list_records(
        OAI_PAGE.replace("<resumptionToken>next-token</resumptionToken>", "")
    )
    fetcher = FakeOaiFetcher([page], [])

    result = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=fetcher,
    ).harvest_oai(
        OaiHarvestRequest(
            client_id="test",
            metadata_prefix="arXiv",
            set_spec="cs:cs:AI",
            mode=OaiHarvestMode.INCREMENTAL,
        )
    )

    assert result.status == "completed"
    assert result.current_watermark == "2026-05-28"
    assert fetcher.seen[0]["from_datestamp"] == "2026-05-28"
    assert fetcher.seen[0]["set_spec"] == "cs:cs:AI"
    watermark = store.get_oai_watermark(metadata_prefix="arXiv", set_spec="cs:cs:AI")
    assert watermark is not None
    assert watermark["last_response_date"] == "2026-05-28"
    assert watermark["last_datestamp_seen"] == "2026-05-27"


def test_oai_incremental_overlap_keeps_from_datestamp_day_granularity(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    settings = settings.model_copy(update={"oai_overlap_seconds": 1})
    store.set_oai_watermark(
        metadata_prefix="arXiv",
        set_spec=None,
        last_response_date="2026-05-28T00:00:00Z",
        last_datestamp_seen=None,
        harvest_id="previous",
    )
    page = parse_oai_pmh_list_records(
        OAI_PAGE.replace("<resumptionToken>next-token</resumptionToken>", "")
    )
    fetcher = FakeOaiFetcher([page], [])

    result = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=fetcher,
    ).harvest_oai(
        OaiHarvestRequest(
            client_id="test",
            metadata_prefix="arXiv",
            mode=OaiHarvestMode.INCREMENTAL,
        )
    )

    assert result.status == "completed"
    assert fetcher.seen[0]["from_datestamp"] == "2026-05-27"


def test_oai_explicit_datestamp_bounds_are_sent_at_day_granularity(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    page = parse_oai_pmh_list_records(
        OAI_PAGE.replace("<resumptionToken>next-token</resumptionToken>", "")
    )
    fetcher = FakeOaiFetcher([page], [])

    result = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=fetcher,
    ).harvest_oai(
        OaiHarvestRequest(
            client_id="test",
            metadata_prefix="arXiv",
            from_datestamp="2020-01-01T12:00:00Z",
            until_datestamp="2020-01-02T23:59:59Z",
            mode=OaiHarvestMode.INCREMENTAL,
        )
    )

    assert result.status == "completed"
    assert fetcher.seen[0]["from_datestamp"] == "2020-01-01"
    assert fetcher.seen[0]["until_datestamp"] == "2020-01-02"
    assert store.get_oai_watermark(metadata_prefix="arXiv", set_spec=None) is None


def test_oai_bounded_replay_does_not_advance_authoritative_watermark(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    store.set_oai_watermark(
        metadata_prefix="arXiv",
        set_spec=None,
        last_response_date="2026-05-28T00:00:00Z",
        last_datestamp_seen="2026-05-27T00:00:00+00:00",
        harvest_id="previous",
    )
    page = parse_oai_pmh_list_records(
        OAI_PAGE.replace("<resumptionToken>next-token</resumptionToken>", "").replace(
            "<datestamp>2026-05-27</datestamp>",
            "<datestamp>2020-01-02</datestamp>",
        )
    )
    fetcher = FakeOaiFetcher([page], [])

    result = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=fetcher,
    ).harvest_oai(
        OaiHarvestRequest(
            client_id="test",
            metadata_prefix="arXiv",
            from_datestamp="2020-01-01",
            until_datestamp="2020-01-02",
            mode=OaiHarvestMode.INCREMENTAL,
        )
    )

    assert result.status == "completed"
    assert result.current_watermark == "2020-01-02"
    assert fetcher.seen[0]["from_datestamp"] == "2020-01-01"
    assert fetcher.seen[0]["until_datestamp"] == "2020-01-02"
    watermark = store.get_oai_watermark(metadata_prefix="arXiv", set_spec=None)
    assert watermark is not None
    assert watermark["last_response_date"] == "2026-05-28T00:00:00Z"
    assert watermark["last_datestamp_seen"] == "2026-05-27T00:00:00+00:00"
    assert watermark["last_successful_harvest_id"] == "previous"


def test_oai_replayed_overlap_is_idempotent_for_papers_and_records(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    page = parse_oai_pmh_list_records(OAI_PAGE.replace("<resumptionToken>next-token</resumptionToken>", ""))
    broker = HuldraBroker(
        store=store,
        settings=settings,
        oai_fetcher=FakeOaiFetcher([page, page], []),
    )

    first = broker.harvest_oai(
        OaiHarvestRequest(client_id="test", metadata_prefix="arXiv", mode=OaiHarvestMode.INITIAL)
    )
    second = broker.harvest_oai(
        OaiHarvestRequest(client_id="test", metadata_prefix="arXiv", mode=OaiHarvestMode.INITIAL)
    )

    with store.connect() as conn:
        records_total = conn.execute("SELECT COUNT(*) FROM oai_records").fetchone()[0]
    assert first.status == "completed"
    assert second.status == "completed"
    assert store.status_summary().papers_total == 1
    assert records_total == 1
