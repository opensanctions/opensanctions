import re
from datetime import datetime
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h


def get_most_recent_link(context, doc):
    links = doc.xpath("//a[contains(@class, 'resource-url-analytics')]")
    dated_links = []

    for link in links:
        href = link.get("href")
        if not href:
            continue

        # Try to find a date in the URL (e.g. 2023.06.05)
        match = re.search(r"(\d{4})[.\-](\d{2})[.\-](\d{2})", href)
        if match:
            date_obj = datetime.strptime(match.group(0), "%Y.%m.%d")
            dated_links.append((date_obj, href))
        # else:
        # context.log.warning(f"Link {href} does not contain a date")
    if not dated_links:
        return None

    # Return the href with the latest date
    return max(dated_links, key=lambda x: x[0])[1]


def crawl_item(row, context):
    id = row.pop("idno")
    name = row.pop("denumirea")
    if not id and not name:
        context.log.warning("Row is missing 'idno', skipping")
        return
    entity = context.make("Organization")
    entity.id = context.make_id(id, name)
    entity.add("name", name)
    entity.add("legalForm", row.pop("forma_juridica"))
    entity.add("country", "md")
    entity.add("address", row.pop("adresa"))
    entity.add("registrationNumber", id)

    # Registration date
    h.apply_date(entity, "incorporationDate", row.pop("data_inreg"))
    # Dissolution (liquidation) date if available
    h.apply_date(entity, "dissolutionDate", row.pop("data_lichidarii"))

    # Person in charge
    if officer := row.pop("conducator"):
        officer = context.make("Person")
        officer.id = context.make_id(entity.id)
        officer.add("name", officer)

        # Link person to the organization
        directorship = context.make("Directorship")
        directorship.id = context.make_id(entity.id, officer.id)
        directorship.add("organization", entity)
        directorship.add("director", officer)

        context.emit(officer)
        context.emit(directorship)

    context.emit(entity)
    context.audit_data(row, ["cuatm"])


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    data_url = get_most_recent_link(context, doc)
    path = context.fetch_resource("list.xlsx", data_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=4):
        crawl_item(item, context)
