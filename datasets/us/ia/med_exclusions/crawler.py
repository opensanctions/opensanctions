from datetime import datetime, timedelta
from typing import Dict

from normality import slugify
from openpyxl import load_workbook
from rigour.mime.types import XLSX

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):

    enrollment_type = row.pop("enrollment_type")
    npi = row.pop("npi")
    license_type = row.pop("state_license_type")
    license_number = row.pop("state_license_number")

    if enrollment_type is None:
        return

    if enrollment_type in {"Individual", "Indivdual"}:
        first_name = row.pop("first_name")
        last_name = row.pop("last_name")
        entity = context.make("Person")
        entity.id = context.make_id(first_name, last_name, npi)
        h.apply_name(entity, first_name=first_name, last_name=last_name)
    elif enrollment_type == "Organization":
        business_name = row.pop("legal_business_name")
        entity = context.make("Organization")
        entity.id = context.make_id(business_name, npi)
        entity.add("name", business_name)
    else:
        context.log.warning("Enrollment type not recognized: " + enrollment_type)
        return

    entity.add("npiCode", npi)
    entity.add("country", "us")
    entity.add("sector", row.pop("specialty"))

    if license_number is not None and license_number != "N/A":
        entity.add(
            "description",
            f"State license type / number: {license_type} / {license_number}",
        )

    sanction_type = row.pop("type_of_sanction")
    sanction_start_date = row.pop("effective_date")
    sanction_end_date = row.pop("sanction_end_date")
    sanction = h.make_sanction(
        context, entity, key=slugify(sanction_type, sanction_start_date)
    )
    h.apply_date(sanction, "startDate", sanction_start_date)
    sanction.add("reason", row.pop("authority"))
    sanction.add("description", sanction_type)

    if sanction_end_date and sanction_end_date not in [
        "Indefinite",
        "Federal Authority",
    ]:
        if sanction_end_date == "2 Years":
            # TODO(Leon Handreke): Maybe use date.replace(year=start_date.year + 2)
            # to more accurately represent the semantics intended by the publisher?
            sanction_end_datetime = datetime.strptime(
                sanction_start_date, "%Y-%m-%d"
            ) + timedelta(days=2 * 365)
            sanction_end_date = sanction_end_datetime.date().isoformat()
        h.apply_date(sanction, "endDate", sanction_end_date)

    is_debarred = h.is_active(sanction)
    if is_debarred:
        entity.add("topics", "debarment")

    context.emit(entity, target=is_debarred)
    context.emit(sanction)

    context.audit_data(
        row,
        ignore=[
            "eligible_to_reapply_date",
        ],
    )


def crawl_excel_url(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    return doc.xpath(".//a[contains(@title, 'Sanctions List')]")[0].get("href")


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=1):
        crawl_item(item, context)
