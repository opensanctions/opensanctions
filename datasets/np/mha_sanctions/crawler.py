import csv
from typing import Dict
import zavod.helpers as h
from zavod import Context

DATE_FORMATS = ["%m/%d/%Y"]


def crawl_row(context: Context, row: Dict[str, str]):
    full_name = row.pop("name")
    # Split `alias` on `;` and trim any extra whitespace
    alias = row.pop("alias").split(";")
    birth_date = h.parse_date(row.pop("date of birth"), DATE_FORMATS)
    listing_date = h.parse_date(row.pop("listed on"), DATE_FORMATS)
    internal_id = row.pop("code")
    address = row.pop("address")
    entity_type = row.pop("type")
    status = row.pop("SANCTION STATUS")

    entity = None
    if entity_type == "INDIVIDUALS":
        entity = context.make("Person")
        entity.id = context.make_id(full_name, birth_date, internal_id)
        entity.add("name", full_name)
        for a in alias:  # Add aliases
            entity.add("alias", a.strip())
        entity.add("birthDate", birth_date)
        entity.add("address", address)

    elif entity_type == "GROUP":
        entity = context.make("Organization")
        entity.id = context.make_id(full_name, address)
        entity.add("name", full_name)
        for a in alias:  # Add aliases
            entity.add("alias", a.strip())
    else:
        context.log.warning("Unhandled entity type", type=entity_type)

    # Proceed only if the entity was created
    if entity:
        if status == "ACTIVE":
            entity.add("topics", "sanction")
            sanction = h.make_sanction(context, entity)
            sanction.add("listingDate", listing_date)
            context.emit(entity, target=True)
            context.emit(sanction)
        elif status == "REMOVED":
            context.emit(entity, target=False)
        else:
            context.emit(entity, target=False)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
