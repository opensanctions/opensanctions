import re
from typing import Dict
import csv
from rigour.mime.types import CSV

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_resource

REGEX_AKA = re.compile(r"\baka\b", re.IGNORECASE)
REGEX_WORD = re.compile(r"\w{2,}")


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

        provider = context.make("LegalEntity")
        provider.id = context.make_id(provider_name, zip_code)

        last_name, first_name = provider_name.split(",", 1)
        names = REGEX_AKA.split(last_name)
        if REGEX_WORD.search(first_name) and REGEX_WORD.search(last_name):
            provider.add_schema("Person")
            h.apply_name(
                provider, last_name=names[0], alias=names[1:], first_name=first_name
            )
        else:
            result = context.lookup("names", provider_name)
            if result is None:
                context.log.warning(
                    "No lookups found for provider", provider_name=provider_name
                )
                provider.add("name", provider_name)
            else:
                provider.add("name", result.name)
                provider.add("alias", result.alias)

        provider.add("country", "us")
        provider.add("topics", "debarment")
        provider.add("address", address)
        sanction = h.make_sanction(context, provider)
        sanction.add("authority", division)

        if provider.get("name"):
            context.emit(provider)
            context.emit(sanction)

    if entity_name := row.pop("Facility Name"):
        # The d/b/a is a person's name and then the company name
        dba_name = None
        if "d/b/a" in entity_name:
            result = context.lookup("names", entity_name)
            if result is not None:
                entity_name = result.name
                dba_name = result.alias
            else:
                context.log.warning(
                    "No lookups found for facility", entity_name=entity_name
                )
        facility = context.make("LegalEntity")  # Sometimes the person's name.
        facility.id = context.make_id(entity_name, zip_code)
        facility.add("name", entity_name)
        facility.add("country", "us")
        facility.add("topics", "debarment")
        facility.add("address", address)
        if dba_name is not None:
            facility.add("alias", dba_name)

        sanction = h.make_sanction(context, facility)
        sanction.add("authority", division)

        context.emit(facility)
        context.emit(sanction)

    if provider.get("name") and entity_name:
        link = context.make("UnknownLink")
        link.id = context.make_id(provider.id, facility.id)
        link.add("object", facility.id)
        link.add("subject", provider.id)
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
