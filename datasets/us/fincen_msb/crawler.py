from csv import DictReader
from typing import Dict, List
from rigour.mime.types import CSV

from zavod import Context
from zavod import helpers as h


def crawl_row(context: Context, row: Dict[str, List[str]]):
    name = row.pop("LEGAL NAME")
    street = row.pop("STREET ADDRESS")
    city = row.pop("CITY")
    state = row.pop("STATE")
    zip_code = row.pop("ZIP")
    country = row.pop("FOREIGN LOCATION")
    listing_date = row.pop("RECEIVED DATE")
    if street and listing_date:  # check to exclude the footnotes
        entity = context.make("LegalEntity")
        entity.id = context.make_id(name, street, listing_date, city, state)
        entity.add("name", name)
        entity.add("alias", row.pop("DBA NAME"))
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
        h.copy_address(entity, address)
        context.emit(entity)
        # context.audit_data(row)


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
