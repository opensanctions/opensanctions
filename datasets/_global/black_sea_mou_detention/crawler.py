from datetime import datetime

from zavod import Context, helpers as h

START_YEAR = 2025
START_MONTH = 1

headers = {
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/x-www-form-urlencoded",
    "Referer": "https://bsis.bsmou.org/public_det/",
    "Origin": "https://bsis.bsmou.org",
    # include cookies or session management if needed
}


def is_future_month(year: int, month: int, now: datetime) -> bool:
    return (year > now.year) or (year == now.year and month > now.month)


def emit_linked_org(context, vessel_id, names, role):
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
    if company_name:
        company = context.make("Company")
        company.id = context.make_id("org", company_name)
        company.add("name", company_name)
        context.emit(company)
        vessel.add("owner", company.id)

    related_ros = clean_row.pop("ros")
    if related_ros:
        emit_linked_org(
            context, vessel.id, related_ros, "Related Recognised Organization"
        )
    class_soc = clean_row.pop("class")
    if class_soc:
        emit_linked_org(context, vessel.id, class_soc, "Classification society")

    sanction = h.make_sanction(
        context,
        vessel,
        start_date=clean_row.pop("date_of_detention"),
        end_date=clean_row.pop("date_of_release", None),
    )
    sanction.add("reason", clean_row.pop("nature_of_deficiencies"))

    if h.is_active(sanction):
        vessel.add("topics", "reg.warn")

    context.emit(vessel)
    context.emit(sanction)

    context.audit_data(clean_row, ["place"])


def crawl(context: Context):
    # now = datetime.utcnow()
    # year = START_YEAR
    # month = START_MONTH
    # while (year, month) <= (now.year, now.month):
    #     # Break if the month is in the future
    #     if is_future_month(year, month, now):
    #         break
    data = {
        "month": "05",  # pad month to two digits
        "year": "2025",  # pad year to four digits
        "auth": "0",
        "held": "0",
    }
    doc = context.fetch_html(
        context.data_url, headers=headers, data=data, method="POST", cache_days=1
    )
    table = doc.xpath("//table[@id='dvData']")
    assert len(table) == 1, "Expected one table in the document"
    table = table[0]
    for row in h.parse_html_table(table):  # , header_tag="th", skiprows=1):
        str_row = h.cells_to_str(row)
        clean_row = {k: v for k, v in str_row.items() if k is not None}
        crawl_row(context, clean_row)

    # # Increment month and roll over year
    # if month == 12:
    #     month = 1
    #     year += 1
    # else:
    #     month += 1
