import xlrd
from followthemoney.types import registry

from zavod import Context
from zavod import helpers as h

# Program name reused from 'us_trade_csl' for consistency
# Refers to the same BIS Denied Persons List program
PROGRAM_NAME = "Denied Persons List (DPL) - Bureau of Industry and Security"


def parse_row(context: Context, row):
    entity = context.make("LegalEntity")
    start_date = row.pop("beginning_date")
    name = row.pop("name")
    country = row.pop("country")

    entity.id = context.make_slug(start_date, name)
    entity.add("name", name)
    entity.add("notes", row.pop("action"))
    entity.add("country", country)
    h.apply_date(entity, "modifiedAt", row.pop("last_update"))

    country_code = registry.country.clean(country)
    address = h.make_address(
        context,
        street=row.pop("street_address"),
        postal_code=row.pop("postal_code"),
        city=row.pop("city"),
        region=row.pop("state"),
        country_code=country_code,
    )
    h.copy_address(entity, address)
    context.emit(entity)

    citation = row.pop("fr_citation")
    # We don't link it to the website here, since it's included in the us_trade_csl
    # programs, which is linked to the website.
    sanction = h.make_sanction(context, entity, key=citation, program_name=PROGRAM_NAME)
    sanction.add("program", citation)
    h.apply_date(sanction, "startDate", start_date)
    h.apply_date(sanction, "endDate", row.pop("ending_date"))
    context.emit(sanction)

    if h.is_active(sanction):
        entity.add("topics", "sanction")

    context.emit(entity)

    context.audit_data(
        row,
        [
            "counter",
            "standard_order",
            "type_of_denial",
            "name_and_address",
        ],
    )


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    url = doc.xpath(".//a[contains(normalize-space(.), 'Export as CSV')]/@href")
    assert len(url) == 1, "Expected exactly one URL"
    path = context.fetch_resource("source.xls", url[0])
    context.export_resource(path, "text/tsv", title=context.SOURCE_TITLE)
    assert xlrd.open_workbook(path).sheet_names() == ["dpl", "Sheet2", "Sheet3"]
    for row in h.parse_xls_sheet(context, xlrd.open_workbook(path)["dpl"]):
        parse_row(context, row)
