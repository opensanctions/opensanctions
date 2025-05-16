from csv import DictReader
from typing import Dict, List
from rigour.mime.types import CSV

from zavod import Context
from zavod import helpers as h

NAME_SPLITS = [
    " DBA ",
    "DBA-",
    "/DBA ",
    " dba ",
]

ALIAS_SPLITS = [
    " dba ",
    "dba ",
    "DBA ",
    "Dba/",
    "DBA: ",
    "DBA:",
    "DBA - ",
    " Dba ",
    "(DBA) ",
    "DBA-",
    "- DBA",
]

IGNORE_COLUMNS = [
    "STATES OF MSB ACTIVITIES",
    "ALL STATES & TERRITORIES & FOREIGN FLAG**",
    "# OF BRANCHES",
    "AUTH SIGN DATE",
]


def crawl_row(context: Context, row: Dict[str, List[str]]):
    name = h.multi_split(row.pop("LEGAL NAME"), NAME_SPLITS)
    street = row.pop("STREET ADDRESS")
    city = row.pop("CITY")
    state = row.pop("STATE")
    zip_code = row.pop("ZIP")
    country = row.pop("FOREIGN LOCATION")
    listing_date = row.pop("RECEIVED DATE")
    if not (street and listing_date):
        return  # check to exclude the footnotes

    country = country or "USA"

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, street, listing_date, city, state)
    entity.add("name", name)
    entity.add(
        "alias",
        h.multi_split(row.pop("DBA NAME"), ALIAS_SPLITS),
    )
    entity.add("sector", h.multi_split(row.pop("MSB ACTIVITIES"), " "))
    entity.add("topics", "fin")
    entity.add("country", country)
    address = h.make_address(
        context,
        street=street,
        city=city,
        state=state,
        postal_code=zip_code,
        country=country,
    )
    h.apply_address(context, entity, address)
    context.emit(entity)
    context.audit_data(row, IGNORE_COLUMNS)


def crawl(context: Context):
    # Perform the POST request with headers
    path = context.fetch_resource(
        "source.tsv",
        "https://msb.fincen.gov/retrieve.msb.list.php",
        data={"site": "AA"},
        method="POST",
    )
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path) as fh:
        reader = DictReader(fh, delimiter="\t")
        for row in reader:
            crawl_row(context, row)
