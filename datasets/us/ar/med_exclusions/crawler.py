import re
from typing import Dict
import csv
from rigour.mime.types import CSV

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_resource

REGEX_AKA = re.compile(r"\baka\b", re.IGNORECASE)
# Handling  ' d/b/aMontessori Day'
REGEX_DBA = re.compile(r"\bd\s*/\s*b\s*/\s*a(?=\w)", re.IGNORECASE)


def crawl_item(row: Dict[str, str], context: Context):
    zip_code = row.pop("Zip")
    address = h.format_address(
        city=row.pop("City"),
        state=row.pop("State"),
        postal_code=zip_code,
        country_code="us",
    )

    division = row.pop("Division")

    if provider_name := row.pop("Provider Name"):
        person = context.make("Person")
        person.id = context.make_id(provider_name, zip_code)

        last_name, first_name = provider_name.split(",", 1)

        names = REGEX_AKA.split(last_name)
        h.apply_name(person, last_name=names[0], alias=names[1:], first_name=first_name)
        person.add("country", "us")
        person.add("topics", "debarment")
        person.add("address", address)
        sanction = h.make_sanction(context, person)
        sanction.add("authority", division)

        context.emit(person, target=True)
        context.emit(sanction)

    if raw_facility_name := row.pop("Facility Name"):
        # The d/b/a is a person's name and then the company name
        names = REGEX_DBA.split(raw_facility_name)
        if len(names) == 2:
            dba_person_name, facility_name = names[0], names[1]
        else:
            facility_name = raw_facility_name
            dba_person_name = None
            if len(names) != 1:
                context.log.warning("More names than expected", raw_facility_name)

        company = context.make("LegalEntity")  # Sometimes the person's name.
        company.id = context.make_id(facility_name, zip_code)
        company.add("name", facility_name)
        if dba_person_name is not None:
            company.add("alias", dba_person_name)
        company.add("country", "us")
        company.add("topics", "debarment")
        company.add("address", address)

        sanction = h.make_sanction(context, company)
        sanction.add("authority", division)

        context.emit(company, target=True)
        context.emit(sanction)

    if provider_name and raw_facility_name:
        link = context.make("UnknownLink")
        link.id = context.make_id(person.id, company.id)
        link.add("object", company)
        link.add("subject", person)
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
