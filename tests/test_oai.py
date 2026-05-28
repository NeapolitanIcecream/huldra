from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx
import pytest

from huldra.broker import HuldraBroker
from huldra.config import HuldraSettings
from huldra.db import HuldraStore
from huldra.fetcher import NonRetryableFetchError, RateLimitedError, TransientFetchError
from huldra.models import OaiHarvestMode, OaiHarvestRequest, OaiMetadataPrefix
from huldra.oai import OaiPmhFetcher, OaiPmhPage, parse_oai_pmh_list_records

OAI_PAGE = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-05-28T00:00:00Z</responseDate>
  <ListRecords>
    <record>
      <header>
        <identifier>oai:arXiv.org:2401.00001</identifier>
        <datestamp>2026-05-27</datestamp>
        <setSpec>cs:cs.AI</setSpec>
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
        <setSpec>cs:cs.AI</setSpec>
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
        <setSpec>cs:cs.AI</setSpec>
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


def test_oai_parser_handles_arxiv_record_deleted_header_error_and_token() -> None:
    page = parse_oai_pmh_list_records(OAI_PAGE)
    deleted = parse_oai_pmh_list_records(OAI_DELETED_PAGE)
    error = parse_oai_pmh_list_records(OAI_ERROR)

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


def test_oai_fetcher_malformed_record_raises_transient_fetch_error(
    settings: HuldraSettings,
) -> None:
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, text=OAI_MALFORMED_RECORD_PAGE)
        )
    )

    with pytest.raises(TransientFetchError) as exc:
        OaiPmhFetcher(settings, client=client).list_records(metadata_prefix="arXiv")

    assert exc.value.status_code == 200
    assert "malformed OAI record" in str(exc.value)


def test_oai_harvest_503_retry_after_persists_oai_cooldown(
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

    rate_state = store.get_rate_state("arxiv_oai_pmh")
    assert result.status == "rate_limited"
    assert result.error_message is not None
    assert "cooldown_until=" in result.error_message
    assert rate_state.cooldown_until is not None
    assert rate_state.last_status == 503


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


def test_oai_harvest_malformed_record_records_failure_and_releases_limiter(
    store: HuldraStore,
    settings: HuldraSettings,
) -> None:
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, text=OAI_MALFORMED_RECORD_PAGE)
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
    assert result.current_watermark == "2026-05-29T00:00:00Z"
    assert fetcher.seen[1]["resumption_token"] == "next-token"
    assert store.get_paper("2401.00001") is not None
    watermark = store.get_oai_watermark(metadata_prefix="arXiv", set_spec=None)
    assert watermark is not None
    assert watermark["last_response_date"] == "2026-05-29T00:00:00Z"


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


def test_oai_incremental_harvest_resumes_from_successful_watermark(
    store: HuldraStore,
    settings: HuldraSettings,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("huldra.broker.time.sleep", lambda _: None)
    store.set_oai_watermark(
        metadata_prefix="arXiv",
        set_spec="cs:cs.AI",
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
            set_spec="cs:cs.AI",
            mode=OaiHarvestMode.INCREMENTAL,
        )
    )

    assert result.status == "completed"
    assert fetcher.seen[0]["from_datestamp"] == "2026-05-28T00:00:00Z"
    assert fetcher.seen[0]["set_spec"] == "cs:cs.AI"


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
    assert result.current_watermark == "2020-01-02T00:00:00+00:00"
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
