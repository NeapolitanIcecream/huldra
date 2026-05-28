from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from urllib.parse import urlparse
from xml.etree import ElementTree

import httpx

from huldra.config import HuldraSettings
from huldra.fetcher import (
    NonRetryableFetchError,
    RateLimitedError,
    TransientFetchError,
    _parse_retry_after_seconds,
)
from huldra.keys import arxiv_version, normalize_arxiv_id
from huldra.models import ArxivPaper, OaiMetadataPrefix, OaiRecord

OAI_NS = "http://www.openarchives.org/OAI/2.0/"
ARXIV_NS = "http://arxiv.org/OAI/arXiv/"
ARXIV_RAW_NS = "http://arxiv.org/OAI/arXivRaw/"


@dataclass(frozen=True, slots=True)
class OaiPmhProtocolError:
    code: str | None
    message: str


@dataclass(frozen=True, slots=True)
class OaiPmhPage:
    records: list[OaiRecord]
    response_date: str | None
    resumption_token: str | None
    errors: list[OaiPmhProtocolError]
    request_params: dict[str, str]


class OaiFetcher(Protocol):
    def list_records(
        self,
        *,
        metadata_prefix: OaiMetadataPrefix,
        set_spec: str | None = None,
        from_datestamp: str | None = None,
        until_datestamp: str | None = None,
        resumption_token: str | None = None,
    ) -> OaiPmhPage: ...


class OaiPmhFetcher:
    def __init__(
        self,
        settings: HuldraSettings,
        *,
        client: httpx.Client | None = None,
    ) -> None:
        self.settings = settings
        self._client = client

    def list_records(
        self,
        *,
        metadata_prefix: OaiMetadataPrefix,
        set_spec: str | None = None,
        from_datestamp: str | None = None,
        until_datestamp: str | None = None,
        resumption_token: str | None = None,
    ) -> OaiPmhPage:
        params = build_list_records_params(
            metadata_prefix=metadata_prefix,
            set_spec=set_spec,
            from_datestamp=from_datestamp,
            until_datestamp=until_datestamp,
            resumption_token=resumption_token,
        )
        headers = {"User-Agent": self.settings.user_agent}
        if self._client is None:
            with httpx.Client(timeout=self.settings.request_timeout_seconds) as client:
                response = self._get_once(client, params, headers)
        else:
            response = self._get_once(self._client, params, headers)

        retry_after = response.headers.get("Retry-After")
        if response.status_code == 429:
            raise RateLimitedError(_parse_retry_after_seconds(retry_after))
        if response.status_code == 503 and retry_after is not None:
            raise RateLimitedError(
                _parse_retry_after_seconds(retry_after),
                "arXiv OAI-PMH returned HTTP 503 with Retry-After",
                status_code=response.status_code,
            )
        if response.status_code >= 500:
            raise TransientFetchError(
                f"arXiv OAI-PMH returned HTTP {response.status_code}",
                status_code=response.status_code,
            )
        if response.status_code < 200 or response.status_code >= 300:
            raise NonRetryableFetchError(
                f"arXiv OAI-PMH returned HTTP {response.status_code}",
                status_code=response.status_code,
            )
        try:
            page = parse_oai_pmh_list_records(response.text, metadata_prefix=metadata_prefix)
        except ElementTree.ParseError as exc:
            raise TransientFetchError(
                "arXiv OAI-PMH returned malformed XML",
                status_code=response.status_code,
            ) from exc
        except ValueError as exc:
            raise TransientFetchError(
                f"arXiv OAI-PMH returned malformed OAI record: {exc}",
                status_code=response.status_code,
            ) from exc
        page = OaiPmhPage(
            records=page.records,
            response_date=page.response_date,
            resumption_token=page.resumption_token,
            errors=page.errors,
            request_params=params,
        )
        non_empty_errors = [error for error in page.errors if error.code != "noRecordsMatch"]
        if non_empty_errors:
            error = non_empty_errors[0]
            raise NonRetryableFetchError(
                f"arXiv OAI-PMH error {error.code or 'unknown'}: {error.message}",
                status_code=response.status_code,
            )
        return page

    def _get_once(
        self,
        client: httpx.Client,
        params: dict[str, str],
        headers: dict[str, str],
    ) -> httpx.Response:
        try:
            return client.get(self.settings.arxiv_oai_pmh_url, params=params, headers=headers)
        except httpx.RequestError as exc:
            raise TransientFetchError(f"arXiv OAI-PMH request failed: {exc}") from exc


def build_list_records_params(
    *,
    metadata_prefix: OaiMetadataPrefix,
    set_spec: str | None = None,
    from_datestamp: str | None = None,
    until_datestamp: str | None = None,
    resumption_token: str | None = None,
) -> dict[str, str]:
    if resumption_token:
        return {"verb": "ListRecords", "resumptionToken": resumption_token}
    params = {"verb": "ListRecords", "metadataPrefix": metadata_prefix}
    if set_spec:
        params["set"] = set_spec
    if from_datestamp:
        params["from"] = from_datestamp
    if until_datestamp:
        params["until"] = until_datestamp
    return params


def parse_oai_pmh_list_records(
    text: str | bytes,
    *,
    metadata_prefix: OaiMetadataPrefix = "arXiv",
) -> OaiPmhPage:
    root = ElementTree.fromstring(text)
    response_date = _text(root.find(f"{{{OAI_NS}}}responseDate"))
    errors = [
        OaiPmhProtocolError(code=element.get("code"), message=_normalize_space(element.text or ""))
        for element in root.findall(f"{{{OAI_NS}}}error")
    ]
    list_records = root.find(f"{{{OAI_NS}}}ListRecords")
    if list_records is None:
        return OaiPmhPage(
            records=[],
            response_date=response_date,
            resumption_token=None,
            errors=errors,
            request_params={},
        )
    records = [
        _record_from_element(record, metadata_prefix=metadata_prefix)
        for record in list_records.findall(f"{{{OAI_NS}}}record")
    ]
    token = _text(list_records.find(f"{{{OAI_NS}}}resumptionToken"))
    return OaiPmhPage(
        records=records,
        response_date=response_date,
        resumption_token=token,
        errors=errors,
        request_params={},
    )


def _record_from_element(
    record: ElementTree.Element,
    *,
    metadata_prefix: OaiMetadataPrefix,
) -> OaiRecord:
    header = record.find(f"{{{OAI_NS}}}header")
    if header is None:
        raise ValueError("OAI record missing header")
    identifier = _text(header.find(f"{{{OAI_NS}}}identifier"))
    if identifier is None:
        raise ValueError("OAI record missing identifier")
    raw_datestamp = _text(header.find(f"{{{OAI_NS}}}datestamp"))
    if raw_datestamp is None:
        raise ValueError("OAI record missing datestamp")
    datestamp = _parse_oai_datestamp(raw_datestamp)
    if datestamp is None:
        raise ValueError(f"OAI record {identifier} has invalid datestamp")
    set_specs = [
        value
        for value in (_text(element) for element in header.findall(f"{{{OAI_NS}}}setSpec"))
        if value
    ]
    deleted = header.get("status") == "deleted"
    metadata = record.find(f"{{{OAI_NS}}}metadata")
    raw_xml = ElementTree.tostring(record, encoding="unicode")
    arxiv_id = _arxiv_id_from_oai_identifier(identifier)
    paper = None
    if not deleted:
        if metadata is None:
            raise ValueError(f"OAI record {identifier} missing metadata")
        paper = _paper_from_oai_metadata(
            metadata,
            metadata_prefix=metadata_prefix,
            identifier=identifier,
            datestamp=datestamp,
            set_specs=set_specs,
            raw_xml=raw_xml,
        )
        arxiv_id = paper.arxiv_id
    return OaiRecord(
        oai_identifier=identifier,
        arxiv_id=arxiv_id,
        metadata_prefix=metadata_prefix,
        datestamp=datestamp,
        set_specs=set_specs,
        deleted=deleted,
        paper=paper,
        raw_xml=raw_xml,
    )


def _paper_from_oai_metadata(
    metadata: ElementTree.Element,
    *,
    metadata_prefix: OaiMetadataPrefix,
    identifier: str,
    datestamp: datetime | None,
    set_specs: list[str],
    raw_xml: str,
) -> ArxivPaper:
    arxiv = metadata.find(f"{{{ARXIV_NS}}}arXiv")
    if arxiv is None:
        raw = metadata.find(f"{{{ARXIV_RAW_NS}}}arXivRaw")
        if raw is None:
            raise ValueError("OAI metadata missing arXiv payload")
        return _paper_from_arxiv_raw(
            raw,
            metadata_prefix=metadata_prefix,
            identifier=identifier,
            datestamp=datestamp,
            set_specs=set_specs,
            raw_xml=raw_xml,
        )
    raw_arxiv_id = _child_text(arxiv, "id", ARXIV_NS) or _arxiv_id_from_oai_identifier(identifier) or ""
    arxiv_id = normalize_arxiv_id(raw_arxiv_id)
    categories = (_child_text(arxiv, "categories", ARXIV_NS) or "").split()
    authors_detail = _authors_detail(arxiv)
    authors = [str(author["name"]) for author in authors_detail if author.get("name")]
    created = _parse_arxiv_date(_child_text(arxiv, "created", ARXIV_NS))
    updated = _parse_arxiv_date(_child_text(arxiv, "updated", ARXIV_NS)) or created
    license_url = _child_text(arxiv, "license", ARXIV_NS)
    links = [
        {"rel": "alternate", "href": f"https://arxiv.org/abs/{arxiv_id}"},
        {"rel": "pdf", "href": f"https://arxiv.org/pdf/{arxiv_id}"},
    ]
    return ArxivPaper(
        arxiv_id=arxiv_id,
        canonical_url=f"https://arxiv.org/abs/{arxiv_id}",
        title=_normalize_space(_child_text(arxiv, "title", ARXIV_NS) or ""),
        abstract=_normalize_space(_child_text(arxiv, "abstract", ARXIV_NS) or "") or None,
        authors=authors,
        authors_detail=authors_detail,
        primary_category=categories[0] if categories else None,
        categories=categories,
        published_at=created,
        updated_at=updated,
        comment=_child_text(arxiv, "comments", ARXIV_NS),
        journal_ref=_child_text(arxiv, "journal-ref", ARXIV_NS),
        doi=_child_text(arxiv, "doi", ARXIV_NS),
        raw_atom={},
        license=license_url,
        oai_identifier=identifier,
        oai_datestamp=datestamp,
        oai_set_specs=set_specs,
        links=links,
        raw_metadata={
            "metadata_prefix": metadata_prefix,
            "raw_xml": raw_xml,
        },
    )


def _paper_from_arxiv_raw(
    raw: ElementTree.Element,
    *,
    metadata_prefix: OaiMetadataPrefix,
    identifier: str,
    datestamp: datetime | None,
    set_specs: list[str],
    raw_xml: str,
) -> ArxivPaper:
    arxiv_id = normalize_arxiv_id(
        _child_text(raw, "id", ARXIV_RAW_NS)
        or _child_text(raw, "id", ARXIV_NS)
        or _arxiv_id_from_oai_identifier(identifier)
        or ""
    )
    title = _raw_child_text(raw, "title") or arxiv_id
    abstract = _raw_child_text(raw, "abstract")
    categories = (_raw_child_text(raw, "categories") or "").split()
    authors = _raw_authors(raw)
    versions = _raw_versions(raw)
    version_dates = [
        _parse_arxiv_date(str(version["date"]))
        for version in versions
        if version.get("date") is not None
    ]
    latest_version = _latest_raw_version_number(versions)
    links = [
        {"rel": "alternate", "href": f"https://arxiv.org/abs/{arxiv_id}"},
        {"rel": "pdf", "href": f"https://arxiv.org/pdf/{arxiv_id}"},
    ]
    return ArxivPaper(
        arxiv_id=arxiv_id,
        version=latest_version,
        canonical_url=f"https://arxiv.org/abs/{arxiv_id}",
        title=_normalize_space(title),
        abstract=_normalize_space(abstract or "") or None,
        authors=authors,
        authors_detail=[{"name": author, "affiliation": None} for author in authors],
        primary_category=categories[0] if categories else None,
        categories=categories,
        published_at=version_dates[0] if version_dates else None,
        updated_at=version_dates[-1] if version_dates else None,
        comment=_raw_child_text(raw, "comments"),
        journal_ref=_raw_child_text(raw, "journal-ref"),
        doi=_raw_child_text(raw, "doi"),
        raw_atom={},
        license=_raw_child_text(raw, "license"),
        oai_identifier=identifier,
        oai_datestamp=datestamp,
        oai_set_specs=set_specs,
        links=links,
        versions=versions,
        raw_metadata={
            "metadata_prefix": metadata_prefix,
            "raw_xml": raw_xml,
        },
    )


def _authors_detail(arxiv: ElementTree.Element) -> list[dict[str, str | None]]:
    authors_parent = arxiv.find(f"{{{ARXIV_NS}}}authors")
    if authors_parent is None:
        return []
    authors = []
    for author in authors_parent.findall(f"{{{ARXIV_NS}}}author"):
        keyname = _child_text(author, "keyname", ARXIV_NS)
        forenames = _child_text(author, "forenames", ARXIV_NS)
        suffix = _child_text(author, "suffix", ARXIV_NS)
        name = _normalize_space(" ".join(part for part in [forenames, keyname, suffix] if part))
        affiliation = _child_text(author, "affiliation", ARXIV_NS)
        if name:
            authors.append({"name": name, "affiliation": affiliation})
    return authors


def _raw_child_text(parent: ElementTree.Element, local_name: str) -> str | None:
    return _child_text(parent, local_name, ARXIV_RAW_NS) or _child_text(parent, local_name, ARXIV_NS)


def _raw_child(parent: ElementTree.Element, local_name: str) -> ElementTree.Element | None:
    child = parent.find(f"{{{ARXIV_RAW_NS}}}{local_name}")
    if child is not None:
        return child
    return parent.find(f"{{{ARXIV_NS}}}{local_name}")


def _raw_authors(raw: ElementTree.Element) -> list[str]:
    authors_parent = _raw_child(raw, "authors")
    if authors_parent is None:
        return []
    nested = [
        _normalize_space(" ".join(child.itertext()))
        for child in list(authors_parent)
        if _normalize_space(" ".join(child.itertext()))
    ]
    if nested:
        return nested
    text = _normalize_space(" ".join(authors_parent.itertext()))
    if not text:
        return []
    if ";" in text:
        return [_normalize_space(part) for part in text.split(";") if _normalize_space(part)]
    if " and " in text:
        return [_normalize_space(part) for part in text.split(" and ") if _normalize_space(part)]
    lines = [_normalize_space(part) for part in text.splitlines() if _normalize_space(part)]
    return lines or [text]


def _raw_versions(raw: ElementTree.Element) -> list[dict[str, str | int | None]]:
    versions_parent = _raw_child(raw, "versions")
    if versions_parent is None:
        return []
    versions: list[dict[str, str | int | None]] = []
    for element in list(versions_parent):
        label = element.get("version") or _raw_child_text(element, "version")
        date = _raw_child_text(element, "date")
        size = _raw_child_text(element, "size")
        source_type = _raw_child_text(element, "source_type")
        number = arxiv_version(label or "")
        version: dict[str, str | int | None] = {
            "version": label,
            "version_number": number,
            "date": date,
        }
        if size is not None:
            version["size"] = size
        if source_type is not None:
            version["source_type"] = source_type
        versions.append(version)
    return versions


def _latest_raw_version_number(versions: list[dict[str, str | int | None]]) -> int | None:
    numbers = [
        value
        for value in (version.get("version_number") for version in versions)
        if isinstance(value, int)
    ]
    return max(numbers) if numbers else None


def _text(element: ElementTree.Element | None) -> str | None:
    if element is None or element.text is None:
        return None
    normalized = _normalize_space(element.text)
    return normalized or None


def _child_text(parent: ElementTree.Element, local_name: str, namespace: str) -> str | None:
    return _text(parent.find(f"{{{namespace}}}{local_name}"))


def _normalize_space(value: str) -> str:
    return " ".join(value.split())


def _parse_oai_datestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = value.replace("Z", "+00:00")
    try:
        if len(raw) == 10:
            return datetime.fromisoformat(raw).replace(tzinfo=UTC)
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _parse_arxiv_date(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%a, %d %b %Y %H:%M:%S %Z"):
        try:
            parsed = datetime.strptime(value, fmt)
        except ValueError:
            continue
        return parsed.replace(tzinfo=UTC)
    return _parse_oai_datestamp(value)


def _arxiv_id_from_oai_identifier(identifier: str) -> str | None:
    raw = identifier.strip()
    if raw.startswith("oai:arXiv.org:"):
        return normalize_arxiv_id(raw.removeprefix("oai:arXiv.org:"))
    parsed = urlparse(raw)
    if parsed.path:
        return normalize_arxiv_id(parsed.path.rsplit("/", 1)[-1])
    return None
