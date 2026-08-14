"""Query CELLAR metadata and retrieve language-specific legal expressions."""

from __future__ import annotations

import base64
import json
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from collections.abc import Iterator, Mapping
from typing import Any, Protocol

import click
import orjson
import requests
from lxml import etree, html
from nomenklatura.cache import Cache
from nomenklatura.db import make_session as make_db_session
from requests import Session

from zavod.helpers.html import xpath_element
from zavod.meta import Dataset
from zavod.meta.http import HTTP
from zavod.runtime.http_ import make_session, request_hash
from zavod.shed.ojeu.celex import eur_lex_url, normalize

CELLAR_RESOURCE_URL = "http://publications.europa.eu/resource/celex/{celex}"
SPARQL_ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
SPARQL_HEADERS = {
    "Accept": "application/sparql-results+json",
    "Content-Type": "application/sparql-query",
}

ACT_QUERY = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX lang: <http://publications.europa.eu/resource/authority/language/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?title ?doc_date ?type ?eli ?amends_celex ?based_on_celex
                ?cons_celex WHERE {
  ?work cdm:resource_legal_id_celex "CELEX_ID"^^xsd:string .
  OPTIONAL { ?work cdm:work_date_document ?doc_date . }
  OPTIONAL { ?work cdm:work_has_resource-type ?type . }
  OPTIONAL { ?work cdm:resource_legal_eli ?eli . }
  OPTIONAL {
    ?expression cdm:expression_belongs_to_work ?work ;
                cdm:expression_uses_language lang:ENG ;
                cdm:expression_title ?title .
  }
  OPTIONAL {
    ?work cdm:resource_legal_amends_resource_legal ?amended_framework .
    ?amended_framework cdm:resource_legal_id_celex ?amends_celex .
  }
  OPTIONAL {
    ?work cdm:resource_legal_based_on_resource_legal ?based_on .
    ?based_on cdm:resource_legal_id_celex ?based_on_celex .
  }
  BIND(COALESCE(?amended_framework, ?work) AS ?framework)
  OPTIONAL {
    ?framework cdm:resource_legal_id_celex ?framework_celex .
    ?consolidated cdm:resource_legal_id_celex ?cons_celex .
    FILTER(STRSTARTS(STR(?cons_celex),
                     CONCAT("0", SUBSTR(STR(?framework_celex), 2), "-")))
  }
}
"""


@dataclass(frozen=True)
class Request:
    """Preserve an exact CELLAR request for cache invalidation and diagnostics."""

    url: str
    method: str = "GET"
    data: bytes | None = None
    headers: tuple[tuple[str, str], ...] = ()

    @property
    def header_map(self) -> dict[str, str]:
        """Build headers for executing or fingerprinting this exact request."""
        return dict(self.headers)


@dataclass(frozen=True)
class Act:
    """Represent source-oriented metadata needed to review an EU legal act."""

    celex: str
    title: str | None
    document_date: str | None
    resource_type: str | None
    eli: str | None
    amends: tuple[str, ...]
    based_on: tuple[str, ...]
    consolidated: tuple[str, ...]
    request: Request

    @property
    def latest_consolidated(self) -> str | None:
        """Select the newest date-suffixed consolidation exposed by CELLAR."""
        return max(self.consolidated, default=None)

    def to_dict(self) -> dict[str, Any]:
        """Return stable JSON-compatible metadata for agents and fixtures."""
        return {
            "amends": list(self.amends),
            "based_on": list(self.based_on),
            "celex": self.celex,
            "consolidated": list(self.consolidated),
            "document_date": self.document_date,
            "eli_url": self.eli,
            "eur_lex_url": eur_lex_url(self.celex),
            "latest_consolidated": self.latest_consolidated,
            "resource_type": self.resource_type,
            "resource_url": cellar_url(self.celex),
            "title": self.title,
        }


@dataclass(frozen=True)
class Expression:
    """Keep fetched source bytes together with reproducibility metadata."""

    celex: str
    language: str
    media_type: str
    request_url: str
    final_url: str
    content: bytes

    @property
    def request(self) -> Request:
        """Return the exact GET request used to fetch this expression."""
        return expression_request(self.celex, self.language, self.media_type)

    @property
    def sha256(self) -> str:
        return sha256(self.content).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        """Describe the expression without embedding its potentially large body."""
        return {
            "bytes": len(self.content),
            "final_url": self.final_url,
            "language": self.language,
            "media_type": self.media_type,
            "request_url": self.request_url,
            "sha256": self.sha256,
        }


class CacheStore(Protocol):
    """Provide the small text-cache surface needed by CELLAR clients."""

    def get(self, key: str, max_age: int | None = None) -> str | None: ...

    def set(self, key: str, value: str | None) -> None: ...

    def delete(self, key: str) -> None: ...


@dataclass(frozen=True)
class _Response:
    final_url: str
    media_type: str | None
    content: bytes

    def dump(self) -> str:
        return json.dumps(
            {
                "content": base64.b64encode(self.content).decode("ascii"),
                "final_url": self.final_url,
                "media_type": self.media_type,
            },
            sort_keys=True,
        )

    @classmethod
    def load(cls, value: str) -> _Response:
        data = json.loads(value)
        return cls(
            final_url=str(data["final_url"]),
            media_type=data.get("media_type"),
            content=base64.b64decode(data["content"]),
        )


class CellarClient:
    """Access CELLAR consistently from crawlers and standalone tools.

    Use this with a crawler's HTTP session and cache, or with ephemeral CLI
    dependencies, so metadata and expression requests share one implementation.
    """

    def __init__(self, session: Session, cache: CacheStore | None = None) -> None:
        self.session = session
        self.cache = cache

    def _fingerprint(self, request: Request) -> str:
        headers: Mapping[str, str] | None = request.header_map or None
        return request_hash(
            request.url,
            method=request.method,
            data=request.data,
            headers=headers,
        )

    def clear(self, request: Request) -> None:
        """Evict a request after its cached response fails validation."""
        if self.cache is not None:
            self.cache.delete(self._fingerprint(request))

    def _fetch(self, request: Request, cache_days: int | None) -> _Response:
        fingerprint = self._fingerprint(request)
        if self.cache is not None and cache_days is not None:
            cached = self.cache.get(fingerprint, max_age=cache_days)
            if cached is not None:
                return _Response.load(cached)

        response = self.session.request(
            request.method,
            request.url,
            data=request.data,
            headers=request.header_map,
        )
        response.raise_for_status()
        media_type = response.headers.get("Content-Type")
        if media_type is not None:
            media_type = media_type.split(";", 1)[0]
        result = _Response(
            final_url=response.url,
            media_type=media_type,
            content=response.content,
        )
        if self.cache is not None and cache_days is not None:
            self.cache.set(fingerprint, result.dump())
        return result

    def query_act(self, value: str, cache_days: int | None = 1) -> Act:
        """Load metadata when reviewing an act or resolving its consolidation."""
        celex = normalize(value)
        request = act_request(celex)
        response = self._fetch(request, cache_days)
        try:
            result = orjson.loads(response.content)
            return parse_act_results(celex, result, request)
        except Exception:
            self.clear(request)
            raise

    def fetch_expression(
        self,
        value: str,
        language: str = "ENG",
        media_type: str = "application/xhtml+xml",
        cache_days: int | None = 1,
    ) -> Expression:
        """Fetch exact source bytes for provenance or legal-text extraction."""
        celex = normalize(value)
        request = expression_request(celex, language, media_type)
        response = self._fetch(request, cache_days)
        if not response.content:
            self.clear(request)
            raise ValueError(f"Empty CELLAR expression for {celex}")
        return Expression(
            celex=celex,
            language=language.upper(),
            media_type=response.media_type or media_type,
            request_url=request.url,
            final_url=response.final_url,
            content=response.content,
        )


def cellar_url(value: str) -> str:
    """Build the content-negotiated CELLAR resource URL for a CELEX."""
    return CELLAR_RESOURCE_URL.format(celex=normalize(value))


def expression_headers(language: str, media_type: str) -> dict[str, str]:
    """Select a language and representation when fetching a CELLAR expression."""
    return {"Accept": media_type, "Accept-Language": language.lower()}


def expression_request(
    value: str,
    language: str = "ENG",
    media_type: str = "application/xhtml+xml",
) -> Request:
    """Build the exact request used to fetch a CELEX expression."""
    return Request(
        cellar_url(value),
        headers=tuple(expression_headers(language, media_type).items()),
    )


def build_act_query(value: str) -> bytes:
    """Build the exact SPARQL request used for metadata and relationships."""
    return ACT_QUERY.replace("CELEX_ID", normalize(value)).encode("utf-8")


def act_request(value: str) -> Request:
    """Build the exact SPARQL request used to inspect an act."""
    return Request(
        SPARQL_ENDPOINT,
        method="POST",
        data=build_act_query(value),
        headers=tuple(SPARQL_HEADERS.items()),
    )


def parse_act_results(value: str, result: Any, request: Request | None = None) -> Act:
    """Parse CELLAR SPARQL JSON into deterministic, deduplicated act metadata."""
    celex = normalize(value)
    try:
        bindings = result["results"]["bindings"]
    except (KeyError, TypeError) as exc:
        raise ValueError("Invalid CELLAR SPARQL response") from exc
    if not bindings:
        raise ValueError(f"CELLAR returned no metadata for {celex}")

    def values(key: str) -> tuple[str, ...]:
        found = {
            str(binding[key]["value"]).strip()
            for binding in bindings
            if key in binding and str(binding[key].get("value", "")).strip()
        }
        return tuple(sorted(found))

    def first(key: str) -> str | None:
        found = values(key)
        return found[0] if found else None

    resource_type = first("type")
    if resource_type is not None:
        resource_type = resource_type.rstrip("/").rsplit("/", 1)[-1]
    return Act(
        celex=celex,
        title=first("title"),
        document_date=first("doc_date"),
        resource_type=resource_type,
        eli=first("eli"),
        amends=values("amends_celex"),
        based_on=values("based_on_celex"),
        consolidated=values("cons_celex"),
        request=request or act_request(celex),
    )


@contextmanager
def cli_client() -> Iterator[CellarClient]:
    """Create an isolated client for commands that run without a crawler Context."""
    dataset = Dataset({"name": "ojeu_cli"})
    db = make_db_session("sqlite:///:memory:")
    session = make_session(HTTP({}))
    try:
        yield CellarClient(session, Cache(db, dataset, create=True))
    finally:
        session.close()
        db.close()


def extract_body_html(expression: Expression) -> bytes:
    """Extract the act body as HTML while retaining its table structure.

    Use this derivative when an analyst or extraction agent needs to work on
    annex tables without the expression's XML declaration, doctype, or head.
    Keep the original expression bytes as the provenance artifact.
    """
    doc = html.fromstring(expression.content)
    body = xpath_element(doc, "//body")
    return etree.tostring(body, encoding="utf-8", method="html")


@click.command(help="Fetch an EU legal expression from CELLAR.")
@click.argument("celex")
@click.option(
    "--language", default="ENG", show_default=True, help="CELLAR language code"
)
@click.option(
    "--media-type",
    default="application/xhtml+xml",
    show_default=True,
    help="Preferred expression media type.",
)
@click.option(
    "--output",
    type=click.Path(path_type=Path, dir_okay=False),
    help="Write the expression to this path instead of standard output.",
)
@click.option(
    "--body-only",
    is_flag=True,
    help="Emit the act body as HTML, preserving annex tables.",
)
def cli(
    celex: str,
    language: str,
    media_type: str,
    output: Path | None,
    body_only: bool,
) -> None:
    """Fetch source bytes for agents and humans without requiring a crawl."""
    try:
        with cli_client() as client:
            expression = client.fetch_expression(
                celex, language=language, media_type=media_type
            )
        content = extract_body_html(expression) if body_only else expression.content
        if output is None:
            click.get_binary_stream("stdout").write(content)
        else:
            output.write_bytes(content)
    except (OSError, ValueError, requests.RequestException) as exc:
        raise click.ClickException(str(exc)) from exc


if __name__ == "__main__":
    cli()
