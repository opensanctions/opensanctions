from datetime import datetime

from zavod import Context, helpers as h

START_YEAR = 2019
START_MONTH = 1


def is_future_month(year: int, month: int, now: datetime) -> bool:
    return (year > now.year) or (year == now.year and month > now.month)


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


def crawl_row(context: Context, clean_row: dict, row: dict):
    ship_name = clean_row.pop("ship_name")
    imo = clean_row.pop("imo_no")
    company_name = clean_row.pop("company")

    vessel = context.make("Vessel")
    vessel.id = context.make_id(ship_name, imo)
    vessel.add("name", ship_name)
    vessel.add("imoNumber", imo)
    vessel.add("flag", clean_row.pop("ship_flag"))
    vessel.add("buildDate", clean_row.pop("year_of_build"))
    vessel.add("grossRegisteredTonnage", clean_row.pop("gross_tonnage"))
    vessel.add("type", clean_row.pop("ship_type"))

    start_date = clean_row.pop("date_of_detention")
    if company_name:
        emit_linked_org(
            context,
            vessel.id,
            company_name,
            "Company",
            start_date,
            "Company",
        )

    related_ros = clean_row.pop("related_ros")
    if related_ros:
        emit_linked_org(
            context,
            vessel.id,
            related_ros,
            "Related Recognised Organization",
            start_date,
            "Organization",
        )
    class_soc = clean_row.pop("classification_society")
    if class_soc:
        emit_linked_org(
            context,
            vessel.id,
            class_soc,
            "Classification society",
            start_date,
            "Organization",
        )

    end_date = clean_row.pop("date_of_release", None)
    reasons_cell = row.pop("nature_of_deficiencies")
    for br in reasons_cell.xpath(".//br"):
        br.tail = br.tail + "\n" if br.tail else "\n"
    reason = reasons_cell.text_content().split("\n")
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

    context.audit_data(clean_row, ["place_of_detention", "nature_of_deficiencies"])


def crawl(context: Context):
    now = datetime.utcnow()
    year = START_YEAR
    month = START_MONTH
    while (year, month) <= (now.year, now.month):
        # Break if the month is in the future
        if is_future_month(year, month, now):
            break
        data = {
            "Mode": "DetList",
            "MOU": "TMOU",
            "Src": "online",
            "Type": "Auth",
            "Month": f"{month:02}",  # pad month to two digits
            "Year": str(year),
            "SaveFile": "",
        }
        doc = context.fetch_html(
            context.data_url, data=data, method="POST", cache_days=1
        )
        table = doc.xpath("//table[@cellspacing=1]")
        assert len(table) == 1, "Expected one table in the document"
        table = table[0]
        for row in h.parse_html_table(table, header_tag="td", skiprows=1):
            str_row = h.cells_to_str(row)
            clean_row = {k: v for k, v in str_row.items() if k is not None}
            crawl_row(context, clean_row, row)

        # Increment month and roll over year
        if month == 12:
            month = 1
            year += 1
        else:
            month += 1
