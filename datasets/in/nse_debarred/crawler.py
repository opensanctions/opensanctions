from rigour.mime.types import XLS
import xlrd

from zavod import Context, helpers as h
from zavod.entity import Entity

SEBI_DEBARRMENT_URL = "https://nsearchives.nseindia.com/content/press/prs_ra_sebi.xls"
OTHER_DEBARRMENT_URL = (
    "https://nsearchives.nseindia.com/content/press/prs_ra_others.xls"
)


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
    context.emit(asset, target=is_debarred)
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

    # It's a target if it wasn't revoked
    period = input_dict.pop("period")
    is_revoked = period and "revoked" in period.lower()
    topics = "reg.warn" if is_revoked else "debarment"

    entity.add("name", name)
    entity.add("jurisdiction", "in")
    if pan and "not provided" not in pan.lower():
        entity.add("taxNumber", pan)
    entity.add("topics", topics)
    din_cin: str = input_dict.pop("din_cin_of_entities_debarred", "")
    if (
        din_cin
        and "not available" not in din_cin.lower()
        and "not provided" not in din_cin.lower()
    ):
        entity.add("description", din_cin)
        din_cin = din_cin.replace("DIN ", "").replace("CIN ", "")
        entity.add("registrationNumber", din_cin.split(" "))

    nse_circular_no = input_dict.pop("nse_circular_no")
    order_date = h.parse_date(input_dict.pop("order_date"), formats=["%Y-%m-%d"])
    order_particulars = input_dict.pop("order_particulars")
    urls = [
        input_dict.pop("source_url"),
        input_dict.pop("nse_circular_no_url", None),
        input_dict.pop("order_particulars_url", None),
    ]

    debarreds.append(entity)

    for debarred in debarreds:
        sanction = h.make_sanction(context, debarred, key=nse_circular_no)
        sanction.add("date", order_date)
        sanction.add("description", "Order Particulars: " + order_particulars)
        sanction.add("duration", period)
        sanction.add("sourceUrl", urls)

        context.emit(entity, target=not is_revoked)
        context.emit(sanction)

    # There is some random data in the 17 and 18 columns
    context.audit_data(
        input_dict,
        ignore=[
            "date_of_nse_circular",
            "column_17",
            "column_17_url",
            "column_18",
            "column_9",
            "column_8",
        ],
    )


def crawl(context: Context):
    items = []
    path_sebi = context.fetch_resource("sebi.xls", SEBI_DEBARRMENT_URL)
    context.export_resource(path_sebi, XLS, title=context.SOURCE_TITLE)
    wb_sebi = xlrd.open_workbook(path_sebi)
    for item in h.parse_xls_sheet(context, wb_sebi["Sheet 1"]):
        item["source_url"] = SEBI_DEBARRMENT_URL
        items.append(item)

    path_other = context.fetch_resource("other.xls", OTHER_DEBARRMENT_URL)
    context.export_resource(path_other, XLS, title=context.SOURCE_TITLE)
    wb_other = xlrd.open_workbook(path_other)
    for item in h.parse_xls_sheet(context, wb_other["Sheet1"]):
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
