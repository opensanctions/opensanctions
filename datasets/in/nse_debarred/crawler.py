import shutil
import zipfile

import xlrd

from rigour.mime.types import XLSX, XLS
import openpyxl

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.shed import zyte_api

SEBI_DEBARRMENT_URL = "https://nsearchives.nseindia.com/content/press/prs_ra_sebi.xls"
OTHER_DEBARRMENT_URL = (
    "https://nsearchives.nseindia.com/content/press/prs_ra_others.xls"
)


def load_sheet(workbook, possible_names):
    for name in possible_names:
        try:
            return workbook[name]
        except KeyError:
            continue
    raise ValueError("None of the worksheet names exist")


def crawl_ownership(
    context: Context, owner: Entity, asset_name: str, is_debarred=False
):
    asset = context.make("LegalEntity")
    asset.id = context.make_id(owner.id, asset_name)
    asset.add("name", asset_name)
    if is_debarred:
        asset.add("topics", "debarment")
    ownership = context.make("Ownership")
    ownership.id = context.make_id("own", owner.id, asset_name)
    ownership.add("owner", owner)
    ownership.add("asset", asset)
    context.emit(ownership)
    context.emit(asset)
    return asset


def crawl_item(input_dict: dict, context: Context):
    name = input_dict.pop("entity_individual_name")
    pan = input_dict.pop("pan", "")
    if name is None:
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, pan)

    asset = None
    debarreds = []

    names = h.multi_split(name, ["Proprietor of", "Owner of"])
    if len(names) == 2:
        name = names[0]
        crawl_ownership(context, entity, names[1])

    names = h.multi_split(name, ["Proprietor", "Owner", "Prop."])
    if len(names) == 2 and names[0].strip():
        name = names[1]
        asset_name = names[0]
        asset_is_debarred = False
        if "and its" in asset_name:
            asset_name = asset_name.replace("and its", "")
            asset_is_debarred = True
        asset = crawl_ownership(
            context, entity, asset_name, is_debarred=asset_is_debarred
        )
        if asset_is_debarred:
            debarreds.append(asset)
    address = None
    names = h.multi_split(name, ["(Address :"])
    if len(names) == 2:
        name = names[0]
        address = names[1].replace(")", "").strip()
        address = address.split(", RTA Folio No:")[0].strip()

    # It's a target if it wasn't revoked
    period = input_dict.pop("period")
    is_revoked = period and "revoked" in period.lower()
    topics = "reg.warn" if is_revoked else "debarment"

    entity.add("name", name)
    if address is not None:
        entity.add("address", address)
    entity.add("jurisdiction", "in")
    if pan and "not provided" not in pan.lower():
        entity.add("taxNumber", pan)
    entity.add("topics", topics)
    din_cin: str = input_dict.pop("din_cin", "")
    if din_cin and "-" not in din_cin:
        entity.add("description", din_cin)
        entity.add("registrationNumber", din_cin.split(" "))

    nse_circular_no = input_dict.pop("nse_circular_no_for_debarment")
    order_date = input_dict.pop("order_date")
    order_particulars = input_dict.pop("order_particulars")
    urls = [
        input_dict.pop("source_url"),
        input_dict.pop("nse_circular_no_for_debarment_url", None),
    ]

    debarreds.append(entity)

    for debarred in debarreds:
        sanction = h.make_sanction(context, debarred, key=nse_circular_no)
        h.apply_date(sanction, "date", order_date)
        sanction.add("description", "Order Particulars: " + order_particulars)
        sanction.add("duration", period)
        sanction.add("sourceUrl", urls)
        if is_revoked:
            h.apply_date(sanction, "endDate", period)

        context.emit(entity)
        context.emit(sanction)

    # There is some random data in the 17 and 18 columns
    context.audit_data(
        input_dict,
        ignore=[
            "date_of_nse_circular",
            "symbol",
            "date_of_nse_circular_for_revocation",
            "date_of_nse_circular_url",
            "nse_circular_no_for_revocation_url",
            "nse_circular_no_for_revocation",
        ],
    )


def parse_xls_or_xlsx_sheet_from_url(context: Context, url: str, filename: str):
    _, _, _, filepath_tmp = zyte_api.fetch_resource(
        context, filename=f"{filename}.temp", url=url, geolocation="in"
    )
    # XLSX is a zipfile internally, sniff for that to detect mimetype
    if zipfile.is_zipfile(filepath_tmp):
        filepath = shutil.move(
            filepath_tmp, context.get_resource_path(f"{filename}.xlsx")
        )
        mimetype = XLSX
        workbook = openpyxl.load_workbook(filepath)
        # One of the sheets is named "Sheet1" and the other is named "Working" in separate files
        sheet = load_sheet(workbook, ["Sheet1", "Working"])
        items = h.parse_xlsx_sheet(context, sheet, extract_links=True)
    else:
        filepath = shutil.move(
            filepath_tmp, context.get_resource_path(f"{filename}.xls")
        )
        mimetype = XLS
        items = h.parse_xls_sheet(context, xlrd.open_workbook(filepath)["Sheet1"])

    context.export_resource(filepath, mimetype, title=context.SOURCE_TITLE)
    return items


def crawl(context: Context):
    items = []

    for item in parse_xls_or_xlsx_sheet_from_url(
        context, SEBI_DEBARRMENT_URL, filename="sebi"
    ):
        item["source_url"] = SEBI_DEBARRMENT_URL
        items.append(item)

    for item in parse_xls_or_xlsx_sheet_from_url(
        context, OTHER_DEBARRMENT_URL, filename="other"
    ):
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
            nse_circular_num = item.get("nse_circular_no_for_debarment")
        else:
            item["order_particulars"] = particulars
            item["nse_circular_no_for_debarment"] = nse_circular_num

        crawl_item(item, context)
