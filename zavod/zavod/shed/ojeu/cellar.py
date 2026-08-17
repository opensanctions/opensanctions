"""Query CELLAR metadata and retrieve language-specific legal expressions."""

from __future__ import annotations

import base64
import json
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from pathlib import Path
from collections.abc import Iterable, Iterator, Mapping
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

RELATED_ACTS_QUERY = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX lang: <http://publications.europa.eu/resource/authority/language/>
PREFIX rt: <http://publications.europa.eu/resource/authority/resource-type/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?celex ?framework_celex ?relation ?title ?doc_date ?type WHERE {
  VALUES ?framework_celex { FRAMEWORK_VALUES }
  ?framework cdm:resource_legal_id_celex ?framework_celex .
  {
    ?work cdm:resource_legal_amends_resource_legal ?framework .
    BIND("amends" AS ?relation)
  }
  UNION
  {
    ?work cdm:resource_legal_based_on_resource_legal ?framework .
    BIND("based_on" AS ?relation)
  }
  ?work cdm:resource_legal_id_celex ?celex ;
        cdm:work_date_document ?doc_date ;
        cdm:work_has_resource-type ?type .
  OPTIONAL {
    ?expression cdm:expression_belongs_to_work ?work ;
                cdm:expression_uses_language lang:ENG ;
                cdm:expression_title ?title .
  }
  FILTER (?doc_date >= "DATE_FROM"^^xsd:date)
  DATE_TO_FILTER
  FILTER (?type IN (RESOURCE_TYPES))
}
ORDER BY ?doc_date ?celex ?framework_celex ?relation
"""

CONSOLIDATIONS_QUERY = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?framework_celex ?cons_celex WHERE {
  VALUES ?framework_celex { FRAMEWORK_VALUES }
  ?framework cdm:resource_legal_id_celex ?framework_celex .
  ?consolidated cdm:resource_legal_id_celex ?cons_celex .
  FILTER(STRSTARTS(STR(?cons_celex),
                   CONCAT("0", SUBSTR(STR(?framework_celex), 2), "-")))
}
"""

RELATED_ACT_TYPES = ("REG_IMPL", "DEC_IMPL", "REG", "DEC")


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
class RelatedAct:
    """Represent one incoming legal relationship to a framework act.

    Use this edge-oriented result for discovery so callers can distinguish an
    amendment from a legal-basis relationship without re-querying CELLAR.
    """

    celex: str
    framework_celex: str
    relation: str
    title: str | None
    document_date: str
    resource_type: str

    def to_dict(self) -> dict[str, str | None]:
        """Return stable JSON fields for crawler issues and command output."""
        return {
            "celex": self.celex,
            "document_date": self.document_date,
            "eur_lex_url": eur_lex_url(self.celex),
            "framework_celex": self.framework_celex,
            "relation": self.relation,
            "resource_type": self.resource_type,
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

    def query_related_acts(
        self,
        framework_celexes: str | Iterable[str],
        date_from: date,
        date_to: date | None = None,
        resource_types: str | Iterable[str] = RELATED_ACT_TYPES,
        cache_days: int | None = 1,
    ) -> tuple[RelatedAct, ...]:
        """Find acts that amend or use known frameworks as their legal basis.

        Use this for bounded discovery runs. The default types cover regulations,
        decisions, and their implementing acts; callers can choose a narrower set.
        """
        request = related_acts_request(
            framework_celexes, date_from, date_to, resource_types
        )
        response = self._fetch(request, cache_days)
        try:
            result = orjson.loads(response.content)
            return parse_related_act_results(result)
        except Exception:
            self.clear(request)
            raise

    def query_consolidations(
        self,
        framework_celexes: str | Iterable[str],
        cache_days: int | None = 1,
    ) -> dict[str, tuple[str, ...]]:
        """List the consolidated versions published for each framework act.

        Use this to check a reviewed snapshot against what CELLAR now offers,
        without a request per framework. Frameworks with no consolidation are
        omitted from the result.
        """
        request = consolidations_request(framework_celexes)
        response = self._fetch(request, cache_days)
        try:
            result = orjson.loads(response.content)
            return parse_consolidations_results(result)
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


def build_related_acts_query(
    framework_celexes: str | Iterable[str],
    date_from: date,
    date_to: date | None = None,
    resource_types: str | Iterable[str] = RELATED_ACT_TYPES,
) -> bytes:
    """Build a bounded inverse-relationship query for framework discovery."""
    values = _normalize_frameworks(framework_celexes)
    if date_to is not None and date_to < date_from:
        raise ValueError("date_to must not be before date_from")
    framework_values = " ".join(f'"{celex}"^^xsd:string' for celex in values)
    date_to_filter = ""
    if date_to is not None:
        date_to_filter = f'FILTER (?doc_date <= "{date_to.isoformat()}"^^xsd:date)'
    type_values = ", ".join(f"rt:{value}" for value in _normalize_types(resource_types))
    query = RELATED_ACTS_QUERY.replace("FRAMEWORK_VALUES", framework_values)
    query = query.replace("DATE_FROM", date_from.isoformat())
    query = query.replace("DATE_TO_FILTER", date_to_filter)
    query = query.replace("RESOURCE_TYPES", type_values)
    return query.encode("utf-8")


def related_acts_request(
    framework_celexes: str | Iterable[str],
    date_from: date,
    date_to: date | None = None,
    resource_types: str | Iterable[str] = RELATED_ACT_TYPES,
) -> Request:
    """Build the exact request for discovering acts related to frameworks."""
    return Request(
        SPARQL_ENDPOINT,
        method="POST",
        data=build_related_acts_query(
            framework_celexes, date_from, date_to, resource_types
        ),
        headers=tuple(SPARQL_HEADERS.items()),
    )


def build_consolidations_query(framework_celexes: str | Iterable[str]) -> bytes:
    """Build a query for the consolidated versions of several framework acts."""
    values = _normalize_frameworks(framework_celexes)
    framework_values = " ".join(f'"{celex}"^^xsd:string' for celex in values)
    query = CONSOLIDATIONS_QUERY.replace("FRAMEWORK_VALUES", framework_values)
    return query.encode("utf-8")


def consolidations_request(framework_celexes: str | Iterable[str]) -> Request:
    """Build the exact request for listing consolidated versions."""
    return Request(
        SPARQL_ENDPOINT,
        method="POST",
        data=build_consolidations_query(framework_celexes),
        headers=tuple(SPARQL_HEADERS.items()),
    )


def _normalize_frameworks(
    framework_celexes: str | Iterable[str],
) -> tuple[str, ...]:
    values = (
        (framework_celexes,)
        if isinstance(framework_celexes, str)
        else tuple(framework_celexes)
    )
    normalized = tuple(sorted({normalize(value) for value in values}))
    if not normalized:
        raise ValueError("At least one framework CELEX is required")
    return normalized


def _normalize_types(resource_types: str | Iterable[str]) -> tuple[str, ...]:
    values = (resource_types,) if isinstance(resource_types, str) else resource_types
    normalized = tuple(sorted({value.strip().upper() for value in values if value}))
    if not normalized:
        raise ValueError("At least one resource type is required")
    for value in normalized:
        if not value.replace("_", "").isalnum():
            raise ValueError(f"Invalid CELLAR resource type: {value}")
    return normalized


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


def parse_related_act_results(result: Any) -> tuple[RelatedAct, ...]:
    """Parse inverse CELLAR relationships into deterministic discovery edges."""
    try:
        bindings = result["results"]["bindings"]
    except (KeyError, TypeError) as exc:
        raise ValueError("Invalid CELLAR SPARQL response") from exc

    found: dict[tuple[str, str, str, str, str], set[str]] = {}
    for binding in bindings:
        try:
            celex = normalize(str(binding["celex"]["value"]))
            framework = normalize(str(binding["framework_celex"]["value"]))
            relation = str(binding["relation"]["value"])
            document_date = str(binding["doc_date"]["value"])
            resource_type = str(binding["type"]["value"]).rstrip("/").rsplit("/", 1)[-1]
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("Invalid related-act binding in CELLAR response") from exc
        if relation not in {"amends", "based_on"}:
            raise ValueError(f"Unsupported CELLAR relationship: {relation}")
        key = (celex, framework, relation, document_date, resource_type)
        title = str(binding.get("title", {}).get("value", "")).strip()
        if title:
            found.setdefault(key, set()).add(title)
        else:
            found.setdefault(key, set())

    related = [
        RelatedAct(
            celex=key[0],
            framework_celex=key[1],
            relation=key[2],
            document_date=key[3],
            resource_type=key[4],
            title=min(titles) if titles else None,
        )
        for key, titles in found.items()
    ]
    return tuple(
        sorted(
            related,
            key=lambda act: (
                act.document_date,
                act.celex,
                act.framework_celex,
                act.relation,
            ),
        )
    )


def parse_consolidations_results(result: Any) -> dict[str, tuple[str, ...]]:
    """Group consolidated versions by the framework act they belong to.

    A framework CELLAR knows nothing about is absent from the mapping rather
    than present with an empty tuple, so callers can tell a missing answer from
    an act that has never been consolidated.
    """
    try:
        bindings = result["results"]["bindings"]
    except (KeyError, TypeError) as exc:
        raise ValueError("Invalid CELLAR SPARQL response") from exc

    found: dict[str, set[str]] = {}
    for binding in bindings:
        try:
            framework = normalize(str(binding["framework_celex"]["value"]))
            consolidated = str(binding["cons_celex"]["value"]).strip()
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(
                "Invalid consolidation binding in CELLAR response"
            ) from exc
        if consolidated:
            found.setdefault(framework, set()).add(consolidated)
    return {celex: tuple(sorted(values)) for celex, values in found.items()}


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
