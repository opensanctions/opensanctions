import csv
from typing import Dict

from zavod import Context
from zavod import helpers as h


def crawl_row(context: Context, row: Dict[str, str]):
    full_name = row.pop("name")
    # Split `other_name` on `/` and trim any extra whitespace
    other_names = row.pop("other name").split("/")
    alias = row.pop("alias")
    notes = row.pop("notes")
    reason = row.pop("reason")
    birth_date = row.pop("date of birth")
    country = row.pop("country")
    birth_place = row.pop("place of birth")
    id_number = row.pop("ID card no.")
    source = row.pop("source")
    entity_type = row.pop("type")
    topics = row.pop("topics")

    entity = None
    if entity_type == "Person":
        entity = context.make("Person")
        entity.id = context.make_id(full_name, birth_place, birth_date)
        entity.add("name", full_name)
        for name in other_names:  # Add other_name parts to alias
            entity.add("alias", name.strip())
        h.apply_date(entity, "birthDate", birth_date)
        entity.add("birthPlace", birth_place, lang="spa")
        entity.add("country", country, lang="deu")
        entity.add("idNumber", id_number)
        entity.add("sourceUrl", source)
    elif entity_type == "Organization":
        entity = context.make("Organization")
        entity.id = context.make_id(full_name, source)
        entity.add("name", full_name)
        for name in other_names:  # Add other_name parts to alias
            entity.add("alias", name.strip())
        entity.add("alias", alias)
        entity.add("notes", notes, lang="deu")
        entity.add("sourceUrl", source)
    else:
        context.log.warning("Unhandled entity type", type=entity_type)

    # Proceed only if the entity was created
    if entity:
        if topics == "sanction":
            entity.add("topics", "sanction")
            sanction = h.make_sanction(context, entity)
            sanction.add("reason", reason, lang="deu")
            context.emit(entity)
            context.emit(sanction)
        elif topics is None:
            context.emit(entity)
        else:
            context.emit(entity)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
