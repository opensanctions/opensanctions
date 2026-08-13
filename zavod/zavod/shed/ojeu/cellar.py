"""Query CELLAR metadata and retrieve language-specific legal expressions."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, Any

import requests
from lxml import etree, html

from zavod.helpers.html import xpath_element
from zavod.shed.ojeu.celex import eur_lex_url, normalize

if TYPE_CHECKING:
    from requests import Session

    from zavod import Context

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
        return expression_request(self.celex)

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


def cellar_url(value: str) -> str:
    """Build the content-negotiated CELLAR resource URL for a CELEX."""
    return CELLAR_RESOURCE_URL.format(celex=normalize(value))


def expression_headers(language: str, media_type: str) -> dict[str, str]:
    """Build identical content-negotiation headers for both transport adapters."""
    return {"Accept": media_type, "Accept-Language": language.lower()}


def expression_request(value: str) -> Request:
    """Build the exact request used to fetch a CELEX expression."""
    return Request(cellar_url(value))


def build_act_query(value: str) -> bytes:
    """Build the exact SPARQL request used for metadata and relationships."""
    return ACT_QUERY.replace("CELEX_ID", normalize(value)).encode("utf-8")


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
    query = build_act_query(celex)
    return Act(
        celex=celex,
        title=first("title"),
        document_date=first("doc_date"),
        resource_type=resource_type,
        eli=first("eli"),
        amends=values("amends_celex"),
        based_on=values("based_on_celex"),
        consolidated=values("cons_celex"),
        request=request or Request(SPARQL_ENDPOINT, method="POST", data=query),
    )


def query_act(context: Context, value: str, cache_days: int = 1) -> Act:
    """Load act metadata through a crawler Context when crawl caching is needed."""
    celex = normalize(value)
    query = build_act_query(celex)
    result = context.fetch_json(
        SPARQL_ENDPOINT,
        method="POST",
        data=query,
        headers=SPARQL_HEADERS,
        cache_days=cache_days,
    )
    return parse_act_results(
        celex, result, Request(SPARQL_ENDPOINT, method="POST", data=query)
    )


def query_act_http(value: str, session: Session | None = None) -> Act:
    """Load act metadata without requiring dataset or Watchful configuration."""
    celex = normalize(value)
    query = build_act_query(celex)
    client = session or requests
    response = client.post(
        SPARQL_ENDPOINT, data=query, headers=SPARQL_HEADERS, timeout=60
    )
    response.raise_for_status()
    return parse_act_results(
        celex,
        response.json(),
        Request(SPARQL_ENDPOINT, method="POST", data=query),
    )


def fetch_expression(
    context: Context,
    value: str,
    language: str = "ENG",
    media_type: str = "application/xhtml+xml",
    cache_days: int = 1,
) -> Expression:
    """Fetch an expression through a crawler Context for cached source access."""
    celex = normalize(value)
    request = expression_request(celex)
    text = context.fetch_text(
        request.url,
        headers=expression_headers(language, media_type),
        cache_days=cache_days,
        method=request.method,
        data=request.data,
    )
    if not text:
        raise ValueError(f"Empty CELLAR expression for {celex}")
    return Expression(
        celex=celex,
        language=language.upper(),
        media_type=media_type,
        request_url=request.url,
        final_url=request.url,
        content=text.encode("utf-8"),
    )


def fetch_expression_http(
    value: str,
    language: str = "ENG",
    media_type: str = "application/xhtml+xml",
    session: Session | None = None,
) -> Expression:
    """Fetch an expression directly for standalone tools and agents."""
    celex = normalize(value)
    request = expression_request(celex)
    client = session or requests
    response = client.get(
        request.url, headers=expression_headers(language, media_type), timeout=60
    )
    response.raise_for_status()
    content_type = response.headers.get("Content-Type", media_type).split(";", 1)[0]
    if not response.content:
        raise ValueError(f"Empty CELLAR expression for {celex}")
    return Expression(
        celex=celex,
        language=language.upper(),
        media_type=content_type,
        request_url=request.url,
        final_url=response.url,
        content=response.content,
    )


def extract_body_html(expression: Expression) -> bytes:
    """Extract the act body as HTML while retaining its table structure.

    Use this derivative when an analyst or extraction agent needs to work on
    annex tables without the expression's XML declaration, doctype, or head.
    Keep the original expression bytes as the provenance artifact.
    """
    doc = html.fromstring(expression.content)
    body = xpath_element(doc, "//body")
    return etree.tostring(body, encoding="utf-8", method="html")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch an EU legal expression from CELLAR."
    )
    parser.add_argument("celex", help="CELEX identifier, EUR-Lex URL, or ELI URL")
    parser.add_argument("--language", default="ENG", help="CELLAR language code")
    parser.add_argument(
        "--media-type",
        default="application/xhtml+xml",
        help="preferred expression media type",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="write the expression to this path instead of standard output",
    )
    parser.add_argument(
        "--body-only",
        action="store_true",
        help="emit the act body as HTML, preserving annex tables",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Fetch source bytes for agents and humans without requiring a crawl."""
    args = _parser().parse_args(argv)
    try:
        expression = fetch_expression_http(
            args.celex, language=args.language, media_type=args.media_type
        )
        content = (
            extract_body_html(expression) if args.body_only else expression.content
        )
        if args.output is None:
            sys.stdout.buffer.write(content)
        else:
            args.output.write_bytes(content)
    except (OSError, ValueError, requests.RequestException) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
