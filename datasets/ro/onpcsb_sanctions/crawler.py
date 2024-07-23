import csv
from typing import Dict
from zavod import Context, helpers as h


def crawl_row(context: Context, row: Dict[str, str]):
    full_name = row.get("name")
    other_name = row.get("other name")
    birth_date = row.get("date of birth")
    birth_place = row.get("place of birth")
    nationality = row.get("nationality")
    passport_number = row.get("passport No.")
    residence = row.get("residence")
    domicile = row.get("domicile")
    fiscal_code = row.get("fiscal code")
    phone_number = row.get("phone number")

    entity = context.make("Person")
    entity.id = context.make_id(full_name, other_name, passport_number)
    entity.add("name", full_name)
    entity.add("alias", other_name)
    entity.add("birthDate", birth_date)
    entity.add("birthPlace", birth_place)
    entity.add("nationality", nationality)
    entity.add("passportNumber", passport_number)
    entity.add("address", residence)
    entity.add("address", domicile)
    entity.add("taxNumber", fiscal_code)
    entity.add("phone", phone_number)
    entity.add("topics", "sanction")
    entity.add("country", "ro")

    sanction = h.make_sanction(context, entity)
    # sanction.id = context.make_id()

    # Emit the entities
    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
