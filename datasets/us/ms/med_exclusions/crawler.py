import openpyxl
from rigour.mime.types import XLSX
from typing import Dict

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):
    if not any(row.values()):  # Skip empty rows
        return

    provider_name = row.pop("provider_name")
    dob = row.pop("date_of_birth")
    npi = row.pop("npi")
    schema = "Person" if dob else "LegalEntity"

    entity = context.make(schema)
    entity.id = context.make_id(provider_name, row.get("npi"))
    entity.add("name", provider_name)
    entity.add("country", "us")
    entity.add("sector", row.pop("provider_type_specialty"))
    entity.add("address", row.pop("provider_address"))
    if entity.schema.name == "Person":
        h.apply_date(entity, "birthDate", dob)
    if npi:
        entity.add("npiCode", npi.split(" "))

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("termination_effective_date"))
    sanction.add("reason", row.pop("termination_reason"))
    exclusion_period_str = row.pop("exclusion_period")
    exclusion_period_lookup_result = context.lookup(
        "exclusion_period", exclusion_period_str
    )
    if exclusion_period_lookup_result:
        sanction.add("endDate", exclusion_period_lookup_result.end_date)
    else:
        exclusion_period_segments = h.multi_split(exclusion_period_str, ["-"])
        # Only apply if it looks like "start_date - end_date"
        if len(exclusion_period_segments) == 2:
            _, end_date_str = exclusion_period_segments
            if end_date_str.lower() != "indefinite":
                h.apply_date(sanction, "endDate", end_date_str)
        else:
            # You will likely want to add an exclusion_period lookup
            context.log.warning(
                'Exclusion period does not look like "start_date - end_date"',
                exclusion_period=exclusion_period_str,
            )

    if h.is_active(sanction):
        entity.add("topics", "debarment")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row, ignore=["column_0", "sanction_type"])


def crawl_data_url(context: Context):
    doc = context.fetch_html(context.data_url, absolute_links=True)
    return doc.xpath("//a[contains(text(), 'Sanctioned Provider List')]/@href")[0]


def crawl(context: Context) -> None:
    data_url = crawl_data_url(context)
    path = context.fetch_resource("source.xlsx", data_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    workbook: openpyxl.Workbook = openpyxl.load_workbook(path, read_only=True)
    assert set(workbook.sheetnames) == set(["Sheet1", "Sheet2", "Sheet3"])
    for row in h.parse_xlsx_sheet(
        context,
        sheet=workbook["Sheet1"],
        skiprows=8,
    ):
        crawl_item(row, context)
