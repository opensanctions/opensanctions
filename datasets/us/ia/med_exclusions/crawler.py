from datetime import datetime, timedelta

from normality import slugify
from openpyxl import load_workbook
from rigour.mime.types import XLSX

from zavod import Context, helpers as h
from zavod.stateful.positions import YEAR_DAYS


def crawl_item(row: dict[str, str | None], context: Context) -> None:
    enrollment_type = row.pop("enrollment_type")
    npi = row.pop("npi")
    license_type = row.pop("state_license_type")
    license_number = row.pop("state_license_number")
    first_name = row.pop("first_name")
    last_name = row.pop("last_name")

    if enrollment_type is None:
        return

    if enrollment_type.lower() in {"individual", "indivdual", "indvidual"}:
        entity = context.make("Person")
        entity.id = context.make_id(first_name, last_name, npi)
        h.apply_name(entity, first_name=first_name, last_name=last_name)
    elif enrollment_type == "Organization":
        business_name = row.pop("legal_business_name")
        entity = context.make("Organization")
        entity.id = context.make_id(business_name, npi)
        entity.add("name", business_name)
        # The source also names an individual behind some organizations
        if first_name or last_name:
            entity.add("alias", h.make_name(first_name=first_name, last_name=last_name))
    else:
        context.log.warning("Enrollment type not recognized: " + enrollment_type)
        return

    entity.add("npiCode", npi)
    entity.add("npiCode", row.pop("affiliated_npi"))
    entity.add("country", "us")
    sector = row.pop("specialty")
    if sector != "N/A":
        entity.add("sector", sector)

    if license_number is not None and license_number != "N/A":
        entity.add(
            "description",
            f"State license type / number: {license_type} / {license_number}",
        )

    sanction_type = row.pop("type_of_sanction")
    sanction_start_date = row.pop("effective_date")
    sanction_end_date = row.pop("sanction_end_date")
    assert sanction_start_date is not None, "Sanction start date is required"
    sanction = h.make_sanction(
        context, entity, key=slugify(sanction_type, sanction_start_date)
    )
    h.apply_date(sanction, "startDate", sanction_start_date)
    sanction.add("reason", row.pop("authority"))
    sanction.add("description", sanction_type)

    if sanction_end_date is not None:
        # The source is inconsistent about the capitalisation of the non-date
        # placeholders it uses in this column (e.g. "2 Years" and "2 years").
        end_date_label = sanction_end_date.lower()
        if end_date_label == "2 years":
            # TODO(Leon Handreke): Maybe use date.replace(year=start_date.year + 2)
            # to more accurately represent the semantics intended by the publisher?
            sanction_end_datetime = datetime.strptime(
                sanction_start_date, "%Y-%m-%d"
            ) + timedelta(days=2 * YEAR_DAYS)
            h.apply_date(sanction, "endDate", sanction_end_datetime.date().isoformat())
        elif end_date_label not in {
            "indefinite",
            "federal authority",
            "on payment plan",
        }:
            h.apply_date(sanction, "endDate", sanction_end_date)

    is_debarred = h.is_active(sanction)
    if is_debarred:
        entity.add("topics", "debarment")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(
        row,
        ignore=["eligible_to_reapply_date", "column_14"],
    )


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, absolute_links=True)
    excel_url = h.xpath_string(doc, ".//a[contains(text(), 'Sanctions List')]/@href")
    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)
    assert wb.active is not None

    for item in h.parse_xlsx_sheet(context, wb.active):
        crawl_item(item, context)
