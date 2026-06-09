import csv
import io
from typing import Any, Dict, List, Optional

from followthemoney import SE
from requests import Response

from zavod.context import Context
from zavod.entity import Entity
from zavod.enrich.permid import PermIDEnricher


class RecordingPermIDEnricher(PermIDEnricher):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.requests: List[List[str]] = []

    def entity_to_query_rows(self, entity: SE) -> List[List[str]]:
        return [[entity.id or "", "", entity.caption, ""]]

    def fetch_match_batch(self, query: bytes) -> Dict[str, Any]:
        reader = csv.reader(
            io.StringIO(query.decode("utf-8")), dialect=csv.unix_dialect
        )
        rows = list(reader)[1:]
        self.requests.append([row[0] for row in rows])
        return {
            "outputContentResponse": [
                {
                    "Input_LocalID": row[0],
                    "Match OpenPermID": f"https://permid.org/1-{row[0]}",
                }
                for row in rows
            ]
        }

    def fetch_perm_org(self, entity: SE, url: str) -> Optional[SE]:
        match = self.make_entity(entity, "Company")
        match.id = "permid-%s" % url.rsplit("-", 1)[-1]
        match.add("name", entity.get("name"))
        match.add("sourceUrl", url)
        return match


def make_company(context: Context, entity_id: str, name: str) -> Entity:
    return Entity.from_data(
        context.dataset,
        {
            "schema": "Company",
            "id": entity_id,
            "properties": {"name": [name]},
        },
    )


def test_permid_api_response_message():
    response = Response()
    response.status_code = 429
    response._content = b'{"message":"API rate limit exceeded"}'
    response.headers["Retry-After"] = "21854"

    assert PermIDEnricher.api_response_message(response) == "API rate limit exceeded"


def test_permid_match_many_batches_requests(vcontext: Context):
    enricher = RecordingPermIDEnricher(
        vcontext.dataset,
        vcontext.cache,
        {"api_token": "token", "cache_days": 0, "batch_size": 2},
    )
    entities = [
        make_company(vcontext, "company-1", "Company One"),
        make_company(vcontext, "company-2", "Company Two"),
        make_company(vcontext, "company-3", "Company Three"),
    ]

    matches = list(enricher.match_many_wrapped(entities))

    assert enricher.requests == [["company-1", "company-2"], ["company-3"]]
    assert [(entity.id, match.id) for entity, match in matches] == [
        ("company-1", "permid-company-1"),
        ("company-2", "permid-company-2"),
        ("company-3", "permid-company-3"),
    ]
