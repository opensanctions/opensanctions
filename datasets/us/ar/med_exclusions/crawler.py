import re
from typing import Dict
import csv
from rigour.mime.types import CSV

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_resource

REGEX_AKA = re.compile(r"\baka\b", re.IGNORECASE)


def crawl_item(row: Dict[str, str], context: Context):
    zip_code = row.pop("Zip")
    division = row.pop("Division")

    address = h.format_address(
        city=row.pop("City"),
        state=row.pop("State"),
        postal_code=zip_code,
        country_code="us",
    )

    if provider_name := row.pop("Provider Name"):
        last_name, first_name = provider_name.split(",", 1)
        names = REGEX_AKA.split(last_name)

        person = context.make("Person")
        person.id = context.make_id(provider_name, zip_code)
        h.apply_name(person, last_name=names[0], alias=names[1:], first_name=first_name)
        person.add("country", "us")
        person.add("topics", "debarment")
        person.add("address", address)
        sanction = h.make_sanction(context, person)
        sanction.add("authority", division)

        context.emit(person)
        context.emit(sanction)

    if entity_name := row.pop("Facility Name"):
        # The d/b/a is a person's name and then the company name
        dba_name = None
        if "d/b/a" in entity_name:
            result = context.lookup("names", entity_name)
            if result is not None:
                # It's a person's name and then the company name
                entity_name, dba_name = result.values[0], result.values[1]
            else:
                context.log.warning("No lookups found for", entity_name)
        entity = context.make("LegalEntity")  # Sometimes the person's name.
        entity.id = context.make_id(entity_name, zip_code)
        entity.add("name", entity_name)
        entity.add("country", "us")
        entity.add("topics", "debarment")
        entity.add("address", address)
        if dba_name is not None:
            entity.add("alias", dba_name)

        sanction = h.make_sanction(context, entity)
        sanction.add("authority", division)

        context.emit(entity)
        context.emit(sanction)

    if provider_name and entity_name:
        link = context.make("UnknownLink")
        link.id = context.make_id(person.id, entity.id)
        link.add("object", entity.id)
        link.add("subject", person.id)
        context.emit(link)

    context.audit_data(row, ignore=[None])


def crawl(context: Context) -> None:
    _, _, _, path = fetch_resource(
        context,
        "source.csv",
        context.data_url,
        expected_media_type=CSV,
        geolocation="us",
    )
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path) as f:
        for item in csv.DictReader(f):
            crawl_item(item, context)
