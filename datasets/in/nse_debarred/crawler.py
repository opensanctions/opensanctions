from typing import Generator
from rigour.mime.types import XLS
from normality import stringify, slugify
from datetime import datetime
import xlrd

from zavod import Context, helpers as h

SEBI_DEBARRMENT_URL = "https://nsearchives.nseindia.com/content/press/prs_ra_sebi.xls"
OTHER_DEBARRMENT_URL = (
    "https://nsearchives.nseindia.com/content/press/prs_ra_others.xls"
)


def parse_sheet(sheet) -> Generator[dict, None, None]:
    headers = None
    for row_ix, row in enumerate(sheet):
        cells = []
        urls = []
        for cell_ix, cell in enumerate(row):
            if cell.ctype == xlrd.XL_CELL_DATE:
                # Convert Excel date format to Python datetime
                date_value = xlrd.xldate_as_datetime(cell.value, sheet.book.datemode)
                cells.append(date_value)
            else:
                cells.append(cell.value)
            url = sheet.hyperlink_map.get((row_ix, cell_ix))
            if url:
                urls.append(url.url_or_path)
        if headers is None:
            headers = []
            for idx, cell in enumerate(cells):
                if not cell:
                    cell = f"column_{idx}"
                headers.append(slugify(cell, "_").lower())
            continue

        record = {}
        for header, value in zip(headers, cells):
            if isinstance(value, datetime):
                value = value.date()
            record[header] = stringify(value)
        if len(record) == 0:
            continue
        if all(v is None for v in record.values()):
            continue
        record["urls"] = urls
        yield record


def crawl_item(input_dict: dict, context: Context):
    name = input_dict.pop("entity_individual_name")
    if name is None:
        return
    pan = input_dict.pop("pan")
    # It's a target if it wasn't revoked
    period = input_dict.pop("period")
    is_revoked = period and "revoked" in period.lower()
    topics = "reg.warn" if is_revoked else "debarment"

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, pan)
    entity.add("name", name)
    entity.add("taxNumber", pan)
    entity.add("topics", topics)
    din_cin: str = input_dict.pop("din_cin_of_entities_debarred", None)
    if din_cin and din_cin.lower != "not available":
        entity.add("description", din_cin)
        din_cin = din_cin.replace("DIN ", "").replace("CIN ", "")
        entity.add("registrationNumber", din_cin.split(" "))

    sanction = h.make_sanction(context, entity, key=input_dict.pop("nse_circular_no"))
    sanction.add(
        "date", h.parse_date(input_dict.pop("order_date"), formats=["%Y-%m-%d"])
    )
    sanction.add(
        "description", "Order Particulars: " + input_dict.pop("order_particulars")
    )
    sanction.add("duration", period)
    sanction.add("sourceUrl", input_dict.pop("source_url"))
    sanction.add("sourceUrl", input_dict.pop("urls"))

    context.emit(entity, target=not is_revoked)
    context.emit(sanction)

    # There is some random data in the 17 and 18 columns
    context.audit_data(
        input_dict,
        ignore=[
            "date_of_nse_circular",
            "column_17",
            "column_18",
            "column_9",
        ],
    )


def crawl(context: Context):
    items = []
    path_sebi = context.fetch_resource("sebi.xls", SEBI_DEBARRMENT_URL)
    context.export_resource(path_sebi, XLS, title=context.SOURCE_TITLE)
    wb_sebi = xlrd.open_workbook(path_sebi)
    for item in parse_sheet(wb_sebi["Sheet 1"]):
        item["source_url"] = SEBI_DEBARRMENT_URL
        items.append(item)

    path_other = context.fetch_resource("other.xls", OTHER_DEBARRMENT_URL)
    context.export_resource(path_other, XLS, title=context.SOURCE_TITLE)
    wb_other = xlrd.open_workbook(path_other)
    for item in parse_sheet(wb_other["Sheet1"]):
        item["source_url"] = OTHER_DEBARRMENT_URL
        items.append(item)

    for item in items:
        # Fill down
        if item.get("order_date"):
            order_date = item.get("order_date")
        else:
            item["order_date"] = order_date

        if item.get("order_particulars"):
            particulars = item.get("order_particulars")
            nse_circular_num = item.get("nse_circular_no")
        else:
            item["order_particulars"] = particulars
            item["nse_circular_no"] = nse_circular_num

        crawl_item(item, context)
