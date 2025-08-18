from datetime import datetime

from zavod import Context, helpers as h


DATA = {
    # last 10 years (we can do it in one request and it's still only 175 ships)
    "from": "01.01.2015",
    "till": datetime.now().strftime("%d.%m.%Y"),
    "auth": "0",
    "held": "0",
}
HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Referer": "https://abuja.marinet.ru/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://abuja.marinet.ru",
}


def emit_linked_org(context, vessel_id, names, role, start_date, schema):
    for name in h.multi_split(names, ";"):
        entity = context.make(schema)
        entity.id = context.make_id("entity", name)
        entity.add("name", name)
        context.emit(entity)

        link = context.make("UnknownLink")
        link.id = context.make_id(vessel_id, entity.id, role)
        link.add("role", role)
        link.add("subject", entity.id)
        link.add("object", vessel_id)
        h.apply_date(link, "date", start_date)
        context.emit(link)


def crawl_row(context: Context, row: dict):
    ship_name = row.pop("Name")
    imo = row.pop("IMO number")
    company_name = row.pop("Company")

    vessel = context.make("Vessel")
    vessel.id = context.make_id(ship_name, imo)
    vessel.add("name", ship_name)
    vessel.add("imoNumber", imo)
    vessel.add("flag", row.pop("Flag"))
    vessel.add("buildDate", row.pop("Year of build"))
    vessel.add("grossRegisteredTonnage", row.pop("Tonnage"))
    vessel.add("type", row.pop("Type"))

    start_date = row.pop("Date of detention")
    if company_name:
        emit_linked_org(
            context,
            vessel.id,
            company_name,
            "Company",
            start_date,
            "Company",
        )

    related_ros = row.pop("Related ROs")
    if related_ros:
        emit_linked_org(
            context,
            vessel.id,
            related_ros,
            "Related Recognised Organization",
            start_date,
            "Organization",
        )
    class_soc = row.pop("Class")
    if class_soc:
        emit_linked_org(
            context,
            vessel.id,
            class_soc,
            "Classification society",
            start_date,
            "Organization",
        )

    end_date = row.pop("Date of release")
    sanction = h.make_sanction(
        context,
        vessel,
        start_date=start_date,
        end_date=end_date,
    )
    reasons = row.pop("Nature of deficiencies")
    for reason in reasons.split(";"):
        sanction.add("reason", reason.strip())
    # Most of the ships (even from 2019) have no end_date, most of them will be marked as active
    if h.is_active(sanction):
        vessel.add("topics", "reg.warn")

    context.emit(vessel)
    context.emit(sanction)

    context.audit_data(row, ["#", "Place"])


def crawl(context: Context):
    doc = context.fetch_html(
        "https://abuja.marinet.ru/public_det/?action=getinspections",
        method="POST",
        data=DATA,
        headers=HEADERS,
    )
    table = doc.xpath("//table[@id='dvData']")
    assert len(table) == 1, "Expected exactly one table"
    table = table[0]
    for row in h.parse_html_table(table, slugify_headers=False):
        str_row = h.cells_to_str(row)
        cleaned_row = {k: v for k, v in str_row.items() if isinstance(k, str)}
        crawl_row(context, cleaned_row)
