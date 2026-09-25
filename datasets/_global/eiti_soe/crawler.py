from typing import Any

from zavod import Context
from zavod import helpers as h

# The Datasette view returns one row per SOE per reporting year, so the same
# company appears several times. Rows for the same company merge on the entity ID.
IGNORE_COLUMNS = [
    "id",
    "year",
    "eiti_id_company",
    "eiti_id_declaration",
    "last_updated",
]


def crawl_row(context: Context, row: dict[str, Any]) -> None:
    name = row.pop("soe_name")
    # The country name is used for ID continuity with the previous source,
    # which only provided the name.
    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, row.pop("country_name"))
    entity.add("name", name)
    entity.add("country", row.pop("country_iso3"))
    entity.add("sector", h.multi_split(row.pop("sectors"), [","]))
    # Placeholder values like "Not available" are mapped to null via type.url lookups.
    entity.add("website", row.pop("public_listing_url"))
    entity.add("sourceUrl", row.pop("audited_statement_url"))
    entity.add("topics", "gov.soe")
    context.audit_data(row, ignore=IGNORE_COLUMNS)

    context.emit(entity)


def crawl(context: Context) -> None:
    next_url: str | None = context.data_url
    params: dict[str, str] | None = {"_shape": "objects", "_size": "max"}
    pages = 0
    while next_url is not None:
        data = context.fetch_json(next_url, params=params)
        rows = data["rows"]
        if len(rows) == 0:
            raise ValueError("No data was returned!")
        for row in rows:
            crawl_row(context, row)

        # Datasette includes the query parameters in next_url.
        next_url = data["next_url"]
        params = None
        pages += 1
        assert pages <= 50, "More pages than expected."
