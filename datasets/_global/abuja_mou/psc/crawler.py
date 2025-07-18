from datetime import datetime, timedelta
from urllib.parse import urlencode

from zavod import Context, helpers as h

TODAY = datetime.today()
HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Referer": "https://abuja.marinet.ru/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://abuja.marinet.ru",
}
SEARCH_DATA = {
    # Go back ~3 years (approximate as 1095 days)
    "From": f"{(TODAY - timedelta(days=1095)).strftime("%d.%m.%Y")}",
    "To": f"{TODAY.strftime("%d.%m.%Y")}",
    "auth": "0",
    "flag": "0",
    "ShipClass": "0",
    "RoCode": "0",
    "Type": "0",
    "Ports": "0",
    "typeinspection": "1",
}


def emit_unknown_link(context, object, subject, role):
    link = context.make("UnknownLink")
    link.id = context.make_id(object, subject, role)
    if role:
        link.add("role", role)
    link.add("subject", subject)
    link.add("object", object)
    context.emit(link)


def crawl_vessel_row(context: Context, str_row: dict):
    ship_name = str_row.pop("ship_name")
    imo = str_row.pop("imo_number")
    vessel = context.make("Vessel")
    vessel.id = context.make_id(ship_name, imo)
    vessel.add("name", ship_name)
    vessel.add("imoNumber", imo)
    vessel.add("type", str_row.pop("type"))
    vessel.add("callSign", str_row.pop("callsign"))
    vessel.add("mmsi", str_row.pop("mmsi"))
    vessel.add("grossRegisteredTonnage", str_row.pop("tonnage"))
    # TODO: map the 'deadweight' once we have a property for it
    # TODO: add the topic (most likely 'mar.control') once we have it
    # https://github.com/opensanctions/followthemoney/issues/1
    vessel.add("flag", str_row.pop("flag"))
    h.apply_date(vessel, "buildDate", str_row.pop("year_of_build"))
    context.emit(vessel)

    class_soc = str_row.pop("classificationsociety")
    if class_soc:
        org = context.make("Organization")
        org.id = context.make_id("org", class_soc)
        org.add("name", class_soc)
        context.emit(org)
        emit_unknown_link(
            context, object=vessel.id, subject=org.id, role="Classification society"
        )

    context.audit_data(str_row, ["deadweight"])


def crawl_vessel_page(context: Context, shipuid: str):
    context.log.debug(f"Processing shipuid: {shipuid}")
    detail_data = {
        "MIME Type": "application/x-www-form-urlencoded",
        "UID": f"{shipuid}",
    }

    # POST to get full ship profile using shipuid
    detail_doc = context.fetch_html(
        f"{context.data_url}/?{urlencode({'action': 'getinsppublic'})}",
        data=detail_data,
        headers=HEADERS,
        method="POST",
        cache_days=182,  # Cache for 6 months
    )
    ship_data = detail_doc.xpath("//h2[text()='Ship data']/following-sibling::table[1]")
    assert len(ship_data) == 1, "Expected exactly one ship data table"
    row = list(h.parse_html_table(ship_data[0]))
    assert len(row) == 1, "Expected exactly one row in ship data table"
    str_row = h.cells_to_str(row[0])
    crawl_vessel_row(context, str_row)


def crawl(context: Context):
    doc = context.fetch_html(
        f"{context.data_url}/?{urlencode({'action': 'getinsppublicall'})}",
        data=SEARCH_DATA,
        headers=HEADERS,
        method="POST",
    )
    shipuids = doc.xpath(
        "///tr[contains(@class, 'even') or contains(@class, 'odd')]//input[@type='hidden']/@value"
    )
    context.log.info(f"Found {len(shipuids)} shipuids in the search response")
    for shipuid in shipuids:
        crawl_vessel_page(context, shipuid)
