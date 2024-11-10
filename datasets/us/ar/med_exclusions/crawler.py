import re
from typing import Dict
import csv
from rigour.mime.types import CSV

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_resource

REGEX_AKA = re.compile(r"\baka\b", re.IGNORECASE)
REGEX_DBA = re.compile(r"\bd\s*/\s*b\s*/\s*a\b", re.IGNORECASE)


def crawl_item(row: Dict[str, str], context: Context):

    address = h.make_address(
        context,
        city=row.pop("City"),
        state=row.pop("State"),
        postal_code=row.pop("Zip"),
    )

    division = row.pop("Division")

    if provider_name := row.pop("Provider Name"):
        person = context.make("Person")
        person.id = context.make_id(provider_name)

        last_name, first_name = provider_name.split(",", 1)

        names = REGEX_AKA.split(last_name)
        h.apply_name(person, last_name=names[0], alias=names[1:], first_name=first_name)
        person.add("country", "us")
        person.add("topics", "debarment")
        if division:
            person.add("sector", division)

        if address:
            h.apply_address(context, person, address)
        sanction = h.make_sanction(context, person)

        context.emit(person, target=True)
        context.emit(sanction)

    if raw_facility_name := row.pop("Facility Name"):

        # The d/b/a is a person's name and then the company name
        names = REGEX_DBA.split(raw_facility_name)
        if len(names) > 1:
            dba_person_name, facility_name = raw_facility_name[0], raw_facility_name[1]
        else:
            facility_name = raw_facility_name
            dba_person_name = None

        company = context.make("Company")
        company.id = context.make_id(facility_name)
        company.add("name", facility_name)
        company.add("country", "us")
        company.add("topics", "debarment")
        if division:
            company.add("sector", division)
        if address:
            h.apply_address(context, company, address)

        if dba_person_name:
            dba_person = context.make("Person")
            dba_person.id = context.make_id(dba_person_name)
            dba_person.add("name", dba_person_name)
            link = context.make("UnknownLink")
            link.id = context.make_id(company.id, dba_person.id)
            link.add("object", company)
            link.add("subject", dba_person)
            link.add("role", "d/b/a")
            context.emit(dba_person)
            context.emit(link)

        sanction = h.make_sanction(context, company)

        context.emit(company, target=True)
        context.emit(sanction)

    if provider_name and raw_facility_name:
        link = context.make("UnknownLink")
        link.id = context.make_id(person.id, company.id)
        link.add("object", company)
        link.add("subject", person)
        context.emit(link)

    if address:
        context.emit(address)
    context.audit_data(row)


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
