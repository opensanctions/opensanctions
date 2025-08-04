import random
import time
from datetime import datetime

from zavod import Context, helpers as h

START_YEAR = 2019
START_MONTH = 1

HEADERS = {
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/x-www-form-urlencoded",
    "Referer": "https://bsis.bsmou.org/public_det/",
    "Origin": "https://bsis.bsmou.org",
}


def emit_linked_org(context, vessel_id, names, role, date):
    for name in h.multi_split(names, ";"):
        org = context.make("Organization")
        org.id = context.make_id("org", name)
        org.add("name", name)
        context.emit(org)

        link = context.make("UnknownLink")
        link.id = context.make_id(vessel_id, org.id, role)
        link.add("role", role)
        link.add("subject", org.id)
        link.add("object", vessel_id)
        h.apply_date(link, "date", date)
        context.emit(link)


def crawl_row(context: Context, clean_row: dict):
    ship_name = clean_row.pop("name")
    imo = clean_row.pop("imo_number")
    company_name = clean_row.pop("company")

    vessel = context.make("Vessel")
    vessel.id = context.make_id(ship_name, imo)
    vessel.add("name", ship_name)
    vessel.add("imoNumber", imo)
    vessel.add("flag", clean_row.pop("flag"))
    vessel.add("buildDate", clean_row.pop("year_of_build"))
    vessel.add("grossRegisteredTonnage", clean_row.pop("tonnage"))
    vessel.add("type", clean_row.pop("type"))
    start_date = clean_row.pop("date_of_detention")
    if company_name:
        company = context.make("Company")
        company.id = context.make_id("org", company_name)
        company.add("name", company_name)
        link = context.make("UnknownLink")
        link.id = context.make_id(vessel.id, company.id, "linked")
        link.add("object", vessel.id)
        link.add("subject", company.id)
        h.apply_date(link, "date", start_date)
        context.emit(company)
        context.emit(link)

    related_ros = clean_row.pop("ros")
    if related_ros:
        emit_linked_org(
            context,
            vessel.id,
            related_ros,
            "Related Recognised Organization",
            start_date,
        )
    class_soc = clean_row.pop("class")
    if class_soc:
        emit_linked_org(
            context,
            vessel.id,
            class_soc,
            "Classification society",
            start_date,
        )

    end_date = clean_row.pop("date_of_release", None)
    reason = clean_row.pop("nature_of_deficiencies").split(";")
    sanction = h.make_sanction(
        context,
        vessel,
        start_date=start_date,
        end_date=end_date,
        key=[start_date, end_date, reason],
    )
    sanction.add("reason", reason)

    if h.is_active(sanction):
        vessel.add("topics", "reg.warn")

    context.emit(vessel)
    context.emit(sanction)
    # "place" is where the vessel was detained
    context.audit_data(clean_row, ["place"])


def crawl(context: Context):
    now = datetime.utcnow()
    year = START_YEAR
    month = START_MONTH
    while (year, month) <= (now.year, now.month):
        data = {
            "month": f"{month:02}",  # pad month to two digits
            "year": str(year),
            "auth": "0",
            "held": "0",
        }
        try:
            doc = context.fetch_html(
                context.data_url,
                headers=HEADERS,
                data=data,
                method="POST",
                cache_days=1,
            )
            table = doc.xpath("//table[@id='dvData']")
            assert len(table) == 1, "Expected one table in the document"
            table = table[0]
            for row in h.parse_html_table(table):  # , header_tag="th", skiprows=1):
                str_row = h.cells_to_str(row)
                clean_row = {k: v for k, v in str_row.items() if k is not None}
                crawl_row(context, clean_row)
        except Exception as e:
            context.log.error(f"Skipping {year}-{month:02} due to fetch failure: {e}")

        # Random sleep to avoid overwhelming the server (and hitting 500 Server Error)
        time.sleep(random.uniform(0.5, 2.0))  # sleep for 1.5–3 seconds

        # Increment month and roll over year
        if month == 12:
            month = 1
            year += 1
        else:
            month += 1
