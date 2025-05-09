import xlrd
from followthemoney.types import registry

from zavod import Context
from zavod import helpers as h


def parse_row(context: Context, row):
    entity = context.make("LegalEntity")
    start_date = row.pop("beginning_date")
    name = row.pop("name")
    country = row.pop("country")

    entity.id = context.make_slug(start_date, name)
    entity.add("name", name)
    entity.add("notes", row.pop("action"))
    entity.add("topics", "sanction")
    entity.add("country", country)
    entity.add("modifiedAt", row.pop("last_update"))

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
    sanction = h.make_sanction(
        context,
        entity,
        key=citation,
        program_name=citation,
        source_program_key=citation,
        # TODO: mappings
        # Map the source program key to the OpenSanctions program key using a lookup (e.g. BE -> BE-FOD-NAT)
        # program_key=(
        #     h.lookup_sanction_program_key(context, citation) if citation else None
        # ),
    )
    sanction.add("program", citation)
    h.apply_date(sanction, "startDate", start_date)
    h.apply_date(sanction, "endDate", row.pop("ending_date"))
    context.emit(sanction)

    context.audit_data(row, ["counter", "standard_order"])


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    url = doc.xpath(".//a[contains(normalize-space(.), 'Export as CSV')]/@href")
    assert len(url) == 1, "Expected exactly one URL"
    path = context.fetch_resource("source.xls", url[0])
    context.export_resource(path, "text/tsv", title=context.SOURCE_TITLE)
    for row in h.parse_xls_sheet(context, xlrd.open_workbook(path)["dpl"]):
        parse_row(context, row)
