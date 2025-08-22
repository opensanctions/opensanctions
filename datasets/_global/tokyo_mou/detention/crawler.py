from datetime import datetime
import re

from lxml.html import HtmlElement

from zavod import Context, helpers as h

START_YEAR = 2019
START_MONTH = 1
REGEX_AMPERSAND = re.compile(r"&? ?amp;", re.IGNORECASE)


def clean_name(name: str) -> str:
    name = REGEX_AMPERSAND.sub("&", name)
    return name


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


def crawl_row(context: Context, row: dict, reasons_cell: HtmlElement):
    ship_name = row.pop("Ship Name")
    imo = row.pop("IMO No.")
    company_name = row.pop("Company")

    vessel = context.make("Vessel")
    vessel.id = context.make_id(ship_name, imo)
    vessel.add("name", ship_name)
    vessel.add("imoNumber", imo)
    vessel.add("flag", row.pop("Ship Flag"))
    vessel.add("buildDate", row.pop("Year of build"))
    vessel.add("grossRegisteredTonnage", row.pop("Gross Tonnage"))
    vessel.add("type", row.pop("Ship Type"))
    vessel.add("topics", "mare.detained")

    start_date = row.pop("Date of detention")
    if company_name:
        emit_linked_org(
            context,
            vessel.id,
            clean_name(company_name),
            "Company",
            start_date,
            "Company",
        )

    related_ros = row.pop("Related ROs")
    if related_ros:
        emit_linked_org(
            context,
            vessel.id,
            clean_name(related_ros),
            "Related Recognised Organization",
            start_date,
            "Organization",
        )
    class_soc = row.pop("Classification society")
    if class_soc:
        emit_linked_org(
            context,
            vessel.id,
            clean_name(class_soc),
            "Classification society",
            start_date,
            "Organization",
        )

    end_date = row.pop("Date of release", None)
    for br in reasons_cell.xpath(".//br"):
        br.tail = br.tail + "\n" if br.tail else "\n"
    reason = reasons_cell.text_content().split("\n")
    sanction = h.make_sanction(
        context,
        vessel,
        start_date=start_date,
        end_date=end_date,
        key=[start_date, end_date, sorted(reason)],
    )
    sanction.add("reason", reason)

    if h.is_active(sanction):
        vessel.add("topics", "reg.warn")

    context.emit(vessel)
    context.emit(sanction)

    context.audit_data(row, ["Place of detention", "Nature of deficiencies", "�"])


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
        for row in h.parse_html_table(
            table, header_tag="td", skiprows=1, slugify_headers=False
        ):
            crawl_row(context, h.cells_to_str(row), row.pop("Nature of deficiencies"))

        # Increment month and roll over year
        if month == 12:
            month = 1
            year += 1
        else:
            month += 1
