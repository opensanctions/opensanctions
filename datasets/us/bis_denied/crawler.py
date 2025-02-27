import csv
from followthemoney.types import registry

from zavod import Context
from zavod import helpers as h


def parse_row(context: Context, row):
    entity = context.make("LegalEntity")
    entity.id = context.make_slug(row.get("Effective_Date"), row.get("Name"))
    entity.add("name", row.get("Name"))
    entity.add("notes", row.get("Action"))
    entity.add("topics", "sanction")
    entity.add("country", row.get("Country"))
    entity.add("modifiedAt", row.get("Last_Update"))

    country_code = registry.country.clean(row.get("Country"))
    address = h.make_address(
        context,
        street=row.get("Street_Address"),
        postal_code=row.get("Postal_Code"),
        city=row.get("City"),
        region=row.get("State"),
        country_code=country_code,
    )
    h.copy_address(entity, address)
    context.emit(entity)

    citation = row.get("FR_Citation")
    sanction = h.make_sanction(context, entity, key=citation)
    sanction.add("program", citation)
    h.apply_date(sanction, "startDate", row.get("Effective_Date"))
    h.apply_date(sanction, "endDate", row.get("Expiration_Date"))
    # pprint(row)
    context.emit(sanction)


def crawl(context: Context):
    path = context.fetch_resource("source.tsv", context.data_url)
    context.export_resource(path, "text/tsv", title=context.SOURCE_TITLE)
    with open(path, "r") as csvfile:
        for row in csv.DictReader(csvfile, delimiter="\t"):
            parse_row(context, row)
