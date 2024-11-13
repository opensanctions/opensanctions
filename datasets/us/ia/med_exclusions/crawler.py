from typing import Dict
from normality import slugify
from rigour.mime.types import XLSX
from openpyxl import load_workbook
from datetime import datetime

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):

    enrollment_type = row.pop("enrollment_type")

    if enrollment_type is None:
        return

    if enrollment_type == "Individual":
        first_name = row.pop("first_name")
        last_name = row.pop("last_name")
        entity = context.make("Person")
        entity.id = context.make_id(first_name, last_name, row.get("npi"))
        h.apply_name(entity, first_name=first_name, last_name=last_name)
    elif enrollment_type == "Organization":
        business_name = row.pop("legal_business_name")
        entity = context.make("Organization")
        entity.id = context.make_id(business_name, row.get("npi"))
        entity.add("name", business_name)
    else:
        context.log.warning("Enrollment type not recognized: " + enrollment_type)
        return

    entity.add("npiCode", row.pop("npi"))
    entity.add("country", "us")

    if row.get("state_license_number") != "N/A":
        entity.add(
            "description",
            "State license type / number: {} / {}".format(
                row.pop("state_license_type"), row.pop("state_license_number")
            ),
        )
    else:
        row.pop("state_license_type")
        row.pop("state_license_number")

    entity.add("sector", row.pop("specialty"))

    sanction_type = row.pop("type_of_sanction")
    effective_date = row.pop("effective_date")
    sanction = h.make_sanction(
        context, entity, key=slugify(sanction_type, effective_date)
    )
    # sanction.add(
    #     "startDate",
    #     h.parse_date(effective_date, formats=["%Y-%m-%d", "%m/%d/%Y"]),
    # )
    h.apply_date(sanction, "startDate", effective_date)
    sanction.add("reason", row.pop("authority"))
    sanction.add("description", sanction_type)

    if row.get("sanction_end_date") and row.get("sanction_end_date") not in [
        "Indefinite",
        "Federal Authority",
    ]:

        is_debarred = (
            datetime.strptime(row.get("sanction_end_date"), "%Y-%m-%d")
            >= datetime.today()
        )

        # sanction.add(
        #     "endDate",
        #     h.parse_date(
        #         row.pop("sanction_end_date"), formats=["%Y-%m-%d", "%m/%d/%Y"]
        #     ),
        # )
        h.apply_date(sanction, "endDate", row.pop("sanction_end_date"))

    else:
        row.pop("sanction_end_date")
        is_debarred = True

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
    return doc.xpath(".//a[contains(@title, 'Sanction List')]")[0].get("href")


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=1):
        crawl_item(item, context)
