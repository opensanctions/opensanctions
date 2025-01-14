import csv
from typing import Dict
import zavod.helpers as h
from zavod import Context


def crawl_row(context: Context, row: Dict[str, str]):
    original_name = row.get("Original Name")
    name_in_brackets = row.get("Name in Brackets")
    country = row.get("Country")
    source_url = row.get("Source URL")

    # Create a Person entity
    entity = context.make("Person")
    entity.id = context.make_id(original_name, name_in_brackets)
    entity.add("name", original_name, lang="lav")
    entity.add("name", name_in_brackets, lang="eng")
    h.apply_date(entity, "birthDate", row.get("Birth Date"))
    entity.add("country", country, lang="eng")
    entity.add("topics", "sanction")

    # Create a Sanction entity
    sanction = h.make_sanction(context, entity)

    # Add source URL
    if source_url:
        entity.add("sourceUrl", source_url.strip())
        sanction.add("sourceUrl", source_url.strip())

    # Emit entities
    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
