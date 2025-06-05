from datetime import datetime

from zavod import Context, helpers as h

YEARS = [2024, 2025]
MONTHS = range(1, 13)


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
    ship_name = clean_row.pop("ship_name")
    imo = clean_row.pop("imo_no")
    company_name = clean_row.pop("company")
    ros = clean_row.pop("related_ros")
    cs = clean_row.pop("classification_society")

    vessel = context.make("Vessel")
    vessel.id = context.make_id(ship_name, imo)
    vessel.add("name", ship_name)
    vessel.add("imoNumber", imo)
    vessel.add("flag", clean_row.pop("ship_flag"))
    vessel.add("buildDate", clean_row.pop("year_of_build"))
    vessel.add("grossRegisteredTonnage", clean_row.pop("gross_tonnage"))
    vessel.add("type", clean_row.pop("ship_type"))
    vessel.add("topics", "reg.warn")
    if company_name:
        company = context.make("Company")
        company.id = context.make_id("org", company_name)
        company.add("name", company_name)
        context.emit(company)
        vessel.add("owner", company.id)
    if ros:
        emit_linked_org(context, vessel.id, ros, "Related Recognised Organization")
    if cs:
        emit_linked_org(context, vessel.id, cs, "Classification society")

    sanction = h.make_sanction(
        context,
        vessel,
        start_date=clean_row.pop("date_of_detention"),
        end_date=clean_row.pop("date_of_release", None),
    )
    sanction.add("reason", clean_row.pop("nature_of_deficiencies"))

    context.emit(vessel)
    context.emit(sanction)

    context.audit_data(clean_row, ["place_of_detention"])


def crawl(context: Context):
    now = datetime.utcnow()
    for year in YEARS:
        for month in MONTHS:
            # Skip months in the future
            if is_future_month(year, month, now):
                continue
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
                crawl_row(context, clean_row)
