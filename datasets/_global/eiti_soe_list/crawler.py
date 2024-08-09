from typing import Any, Dict
from zavod import Context


def crawl_entity(context: Context, item: Dict[str, Any]) -> None:
    entity = context.make("LegalEntity")

    soe = item.pop("SOE")
    year = item.pop("Year")
    country = item.pop("Country")
    fin_statement = item.pop("Audited Financial Statement or Equivalent")
    website = item.pop("Public Listing or Website")

    # Making a unique ID for the entity
    entity.id = context.make_id(soe, year, country)

    # Adding fields to the entity
    entity.add("name", soe)
    entity.add("country", country)
    entity.add("sector", item.pop("Sector"))
    entity.add("description", item.pop("Commodities"))
    if website and website.lower() not in {"n/a", "n/v"}:
        entity.add("website", website)
    if fin_statement and fin_statement.lower() not in {"n/a", "n/v"}:
        entity.add("notes", fin_statement)
    entity.add("topics", "gov.soe")

    context.emit(entity, target=True)


def crawl(context: Context):
    data = context.fetch_json(context.data_url)
    if not data or not data.get("rows"):
        context.log.error("No data found or 'rows' key is missing.")
        return

    # Processing each item in the 'rows' list
    for item in data["rows"]:
        crawl_entity(context, item)
