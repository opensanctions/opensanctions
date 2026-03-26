import random
import time
from datetime import datetime
from datetime import timezone
from typing import Any
from urllib.parse import urlencode

from lxml import html

from zavod import Context, helpers as h
from zavod.extract import zyte_api

START_YEAR = 2019
START_MONTH = 1


def emit_linked_org(
    context: Context,
    *,
    vessel_id: str | None,
    names: str,
    role: str,
    date: str | None,
) -> None:
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


def crawl_row(context: Context, row: dict[str, Any]) -> None:
    ship_name = row.pop("Name")
    imo = row.pop("IMO number")
    company_name = row.pop("Company")

    vessel = context.make("Vessel")
    vessel.id = context.make_id(ship_name, imo)
    vessel.add("name", ship_name)
    vessel.add("imoNumber", imo)
    vessel.add("flag", row.pop("Flag"))
    vessel.add("buildDate", row.pop("Year of build"))
    h.apply_number(vessel, "grossRegisteredTonnage", row.pop("Tonnage"))
    vessel.add("type", row.pop("Type"))
    vessel.add("topics", "mare.detained")
    start_date = row.pop("Date of detention")
    if company_name:
        company = context.make("Company")
        # We're adding the IMO number to the company ID to help disambiguate companies with the same name
        # (e.g. https://www.opensanctions.org/entities/bs-mou-0bbe47c69066cbcf4bdad28569acb46252a4b138/)
        company.id = context.make_id("org", company_name, imo)
        company.add("name", company_name)
        link = context.make("UnknownLink")
        link.id = context.make_id(vessel.id, company.id, "linked")
        link.add("object", vessel.id)
        link.add("subject", company.id)
        link.add("role", "Associated Company")
        h.apply_date(link, "date", start_date)
        context.emit(company)
        context.emit(link)

    related_ros = row.pop("ROs")
    if related_ros:
        emit_linked_org(
            context,
            vessel_id=vessel.id,
            names=related_ros,
            role="Related Recognised Organization",
            date=start_date,
        )
    class_soc = row.pop("Class")
    if class_soc:
        emit_linked_org(
            context,
            vessel_id=vessel.id,
            names=class_soc,
            role="Classification society",
            date=start_date,
        )

    end_date = row.pop("Date of release", None)
    reason = row.pop("Nature of deficiencies").split(";")
    sanction = h.make_sanction(
        context,
        vessel,
        start_date=start_date,
        end_date=end_date,
        # key is a str, but we suppress the warning for now to avoid a delta
        # we can re-key in the future if desired
        key=[start_date, end_date, reason],  # type: ignore
    )
    sanction.add("reason", reason)

    if h.is_active(sanction):
        vessel.add("topics", "reg.warn")

    context.emit(vessel)
    context.emit(sanction)
    # "place" is where the vessel was detained
    context.audit_data(row, ["Place", "#"])


def crawl(context: Context) -> None:
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": context.data_url,
        "Origin": context.data_url,
    }
    now = datetime.now(tz=timezone.utc)
    year = START_YEAR
    month = START_MONTH
    while (year, month) <= (now.year, now.month):
        data = {
            "month": f"{month:02}",  # pad month to two digits
            "year": str(year),
            "auth": "0",
            "held": "0",
        }
        zyte_result = zyte_api.fetch(
            context,
            zyte_api.ZyteAPIRequest(
                url=context.data_url,
                headers=headers,
                body=urlencode(data).encode("utf-8"),
                method="POST",
            ),
            cache_days=1,
        )

        try:
            doc = html.fromstring(zyte_result.response_text)
            table = h.xpath_element(doc, "//table[@id='dvData']")
        except Exception:
            if zyte_result:
                context.cache.delete(zyte_result.cache_fingerprint)
            context.log.exception(
                "Failed to fetch HTML or find table for month", month=month, year=year
            )
            continue

        for row in h.parse_html_table(table, slugify_headers=False):
            crawl_row(context, h.cells_to_str(row))

        # Random sleep to avoid overwhelming the server (and hitting 500 Server Error)
        time.sleep(random.uniform(0.5, 2.0))  # sleep for 1.5–3 seconds

        # Increment month and roll over year
        if month == 12:
            month = 1
            year += 1
        else:
            month += 1
