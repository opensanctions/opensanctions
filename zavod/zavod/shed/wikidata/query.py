from typing import Dict
from jinja2 import Template
from pathlib import Path

from zavod.context import Context

from zavod.shed.wikidata.struct import SparqlResponse


queries_path = Path(__file__).resolve().parent / "queries"

ENDPOINT = "https://query.wikidata.org/sparql"
HEADERS = {
    "Accept": "application/sparql-results+json",
}
CACHE_SHORT = 1
CACHE_MEDIUM = CACHE_SHORT * 7
CACHE_LONG = CACHE_SHORT * 30


def make_query(name: str, variables: Dict[str, str]) -> str:
    query_path = queries_path.joinpath(f"{name}.sparql")
    with open(query_path, "r") as fh:
        query_tmpl = fh.read()

    template = Template(query_tmpl)
    return template.render(**variables)


def run_raw_query(
    context: Context,
    query_text: str,
    cache_days: int = CACHE_SHORT,
) -> SparqlResponse:
    params = {"query": query_text}
    data = context.fetch_json(
        ENDPOINT,
        params=params,
        headers=HEADERS,
        cache_days=cache_days,
    )
    return SparqlResponse(query_text, data)


def run_query(
    context: Context,
    name: str,
    variables: Dict[str, str] = {},
    cache_days: int = CACHE_SHORT,
) -> SparqlResponse:
    query_text = make_query(name, variables)
    return run_raw_query(context, query_text, cache_days)
