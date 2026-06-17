import openpyxl
from rigour.mime.types import XLSX

from zavod import Context, helpers as h


INDEFINITE_END_DATE = "2999-12-31"


def crawl_item(row: dict[str, str | None], context: Context) -> None:
    org_name = row.pop("organization_name")
    first_name = row.pop("first_name")
    middle_name = row.pop("middle_name")
    last_name = row.pop("last_name")
    suffix = row.pop("suffix")

    if not any(v and v.strip() for v in (org_name, first_name, last_name)):
        return

    entity_type = row.pop("entity_type")
    npi = row.pop("npi")
    medicaid_id = row.pop("medicaid_id")
    dob = row.pop("date_of_birth")

    if entity_type == "Organization":
        schema = "LegalEntity"
    elif entity_type == "Individual":
        schema = "Person"
    elif first_name or last_name:
        schema = "Person"
    elif org_name:
        schema = "LegalEntity"
    else:
        context.log.warning("Could not determine entity schema", row=row)
        return

    entity = context.make(schema)
    if schema == "Person":
        entity.id = context.make_id(first_name, last_name, npi, dob)
        h.apply_name(
            entity,
            first_name=first_name,
            middle_name=middle_name,
            last_name=last_name,
            suffix=suffix,
        )
        h.apply_date(entity, "birthDate", dob)
    else:
        entity.id = context.make_id(org_name, npi)
        entity.add("name", org_name)

    entity.add("country", "us")
    entity.add("sector", row.pop("provider_type"))
    entity.add("sector", row.pop("speciality"))
    if npi:
        entity.add("npiCode", npi.split(" "))

    address = h.make_address(
        context,
        street=row.pop("address_line_1"),
        street2=row.pop("address_line_2"),
        city=row.pop("city"),
        state=row.pop("state"),
        postal_code=row.pop("zipcode"),
        country_code="us",
    )
    h.apply_address(context, entity, address)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("termination_effective_date"))
    end_date = row.pop("termination_end_date")
    if end_date and end_date != INDEFINITE_END_DATE:
        h.apply_date(sanction, "endDate", end_date)
    sanction.add("reason", row.pop("termination_reason"))
    sanction.add("provisions", row.pop("sanction_type"))
    sanction.add("description", row.pop("additional_notes"))
    if medicaid_id:
        sanction.add("recordId", medicaid_id)

    if h.is_active(sanction):
        entity.add("topics", "debarment")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row, ignore=["column_0", "role", "exclusion_period"])


def crawl_data_url(context: Context) -> str:
    doc = context.fetch_html(context.data_url, absolute_links=True)
    url = h.xpath_string(doc, "//a[contains(text(), 'Sanctioned Provider List')]/@href")
    assert url is not None, "Could not find Excel file URL"
    return url


def crawl(context: Context) -> None:
    data_url = crawl_data_url(context)
    path = context.fetch_resource("source.xlsx", data_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    workbook: openpyxl.Workbook = openpyxl.load_workbook(path, read_only=True)
    assert set(workbook.sheetnames) == set(["Sheet1", "Sheet2", "Sheet3"])
    header_lookup = context.dataset.lookups["headers"]
    for row in h.parse_xlsx_sheet(
        context,
        sheet=workbook["Sheet1"],
        skiprows=8,
        header_lookup=header_lookup,
    ):
        crawl_item(row, context)
