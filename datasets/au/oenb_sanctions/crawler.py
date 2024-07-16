import csv
from typing import Dict
import zavod.helpers as h
from zavod import Context

# Define constants for date parsing
DATE_FORMATS = ["%d. %m. %Y"]


def crawl_row(context: Context, row: Dict[str, str]):
    """Process one row of the CSV data"""
    full_name = h.make_name(row.get("name"))
    if full_name is not None and "," in full_name:
        last_name, first_name = map(str.strip, full_name.split(",", 1))
        h.make_name(first_name=first_name, last_name=last_name)
    #  name_in_brackets = row.get("alias")
    birth_date = h.parse_date(row.get("date of birth"), DATE_FORMATS)
    birth_place = row.get("place of birth")
    passport_number = row.get("ID card no.")
    reason = row.get("reason")

    # Create a Person entity
    entity = context.make("Person")
    h.apply_name(entity, first_name=first_name, last_name=last_name)
    entity.id = context.make_id(first_name, birth_date)

    # Add names
    # entity.add("name", original_name, lang="spa")
    # entity.add("alias", name_in_brackets, lang="eng")
    entity.add("birthDate", birth_date)
    entity.add("birthPlace", birth_place, lang="eng")
    entity.add("passportNumber", passport_number)
    entity.add("topics", "sanction")
    # entity.add()

    # Create a Sanction entity
    sanction = h.make_sanction(context, entity)
    sanction.add("reason", reason, lang="deu")

    # Add source URL
    #   entity.add("sourceUrl", source_url.strip())
    #  sanction.add("sourceUrl", source_url.strip())

    # Emit entities
    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
