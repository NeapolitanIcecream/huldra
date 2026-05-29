from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx
import pytest

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
    ) -> OaiPmhPage:
        self.seen.append(
            {
                "metadata_prefix": metadata_prefix,
                "set_spec": set_spec,
                "from_datestamp": from_datestamp,
                "until_datestamp": until_datestamp,
                "resumption_token": resumption_token,
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
