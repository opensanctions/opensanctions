import re
from datetime import datetime, timedelta

from normality import slugify
from openpyxl import load_workbook
from rigour.mime.types import XLSX

from zavod import Context, helpers as h
from zavod.stateful.positions import YEAR_DAYS

# Some sanction end dates state a duration ("1 year", "2 Years") instead of a date.
DURATION_YEARS = re.compile(r"^(\d+)\s+years?$", re.IGNORECASE)
ISO_DATE_LENGTH = len("YYYY-MM-DD")


def crawl_item(row: dict[str, str | None], context: Context) -> None:
    enrollment_type = row.pop("enrollment_type")
    npi = row.pop("npi")
    license_type = row.pop("state_license_type")
    license_number = row.pop("state_license_number")

    if enrollment_type is None:
        return

    if enrollment_type.lower() in {"individual", "indivdual", "indvidual"}:
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
        # A few organization rows also name the individual provider behind the
        # business. The sanction applies to the enrolled organization, so the
        # person is only emitted as a related entity.
        first_name = row.pop("first_name")
        last_name = row.pop("last_name")
        if first_name is not None or last_name is not None:
            person = context.make("Person")
            person.id = context.make_id(first_name, last_name, npi)
            h.apply_name(person, first_name=first_name, last_name=last_name)
            person.add("country", "us")
            link = context.make("UnknownLink")
            link.id = context.make_id(person.id, entity.id)
            link.add("subject", person)
            link.add("object", entity)
            context.emit(person)
            context.emit(link)
    else:
        context.log.warning("Enrollment type not recognized: " + enrollment_type)
        return

    entity.add("npiCode", npi)
    entity.add("npiCode", row.pop("affiliated_npi"))
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
    assert sanction_start_date is not None, "Sanction start date is required"
    sanction = h.make_sanction(
        context, entity, key=slugify(sanction_type, sanction_start_date)
    )
    h.apply_date(sanction, "startDate", sanction_start_date)
    sanction.add("reason", row.pop("authority"))
    sanction.add("description", sanction_type)

    # Values which mean the exclusion is open-ended, such as "Indefinite", are
    # dropped by the type.date lookup.
    if sanction_end_date is not None:
        duration = DURATION_YEARS.match(sanction_end_date)
        if duration is not None:
            # TODO(Leon Handreke): Maybe use date.replace(year=start_date.year + n)
            # to more accurately represent the semantics intended by the publisher?
            start_dates = sanction.get("startDate")
            if len(start_dates) != 1 or len(start_dates[0]) != ISO_DATE_LENGTH:
                context.log.warning(
                    "Cannot anchor a sanction duration without a full start date",
                    duration=sanction_end_date,
                    start_date=sanction_start_date,
                )
            else:
                years = int(duration.group(1))
                sanction_end_datetime = datetime.strptime(
                    start_dates[0], "%Y-%m-%d"
                ) + timedelta(days=years * YEAR_DAYS)
                h.apply_date(
                    sanction,
                    "endDate",
                    sanction_end_datetime.date().isoformat(),
                    original_value=sanction_end_date,
                )
        else:
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

    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=1):
        crawl_item(item, context)
