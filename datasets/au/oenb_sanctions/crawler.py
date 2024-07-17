import csv
from typing import Dict
import zavod.helpers as h
from zavod import Context

# Define constants for date parsing
DATE_FORMATS = ["%d. %m. %Y"]


def crawl_row(context: Context, row: Dict[str, str]):
    full_name = row.get("name")
    alias = row.get("alias")
    birth_date = h.parse_date(row.get("date of birth"), DATE_FORMATS)
    birth_place = row.get("place of birth")
    passport_number = row.get("ID card no.")
    reason = row.get("reason")

    # Create a Person entity
    if row.get("type") == "Person":
        # if full_name and "," in full_name:
        #     last_name, first_name = map(str.strip, full_name.split(",", 1))
        # else:
        #     first_name, last_name = None, full_name  # Adjust based on contextual needs

        # entity = context.make("Person")
        # if first_name and last_name:
        #     h.apply_name(entity, first_name=first_name, last_name=last_name)
        #     entity.id = context.make_id(first_name, last_name)
        entity = context.make("Person")
        entity.id = context.make_id(full_name)
    entity.add("birthDate", birth_date)
    entity.add("birthPlace", birth_place, lang="eng")
    entity.add("passportNumber", passport_number)

    # Create an Organization entity if it's an organization
    if row.get("type") == "Organization":
        entity = context.make("Organization")
        entity.id = context.make_id(full_name)
        entity.add("name", full_name)
        entity.add("otherNames", row.get("other name"))
        entity.add("alias", alias, lang="eng")
        entity.add("birthPlace", birth_place, lang="eng")
        entity.add("passportNumber", passport_number)

    if entity:
        entity.add("topics", "sanction")

    sanction = h.make_sanction(context, entity)
    sanction.add("reason", reason, lang="deu")

    # Emit entities
    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
