import re
import shutil
import zipfile
from pathlib import Path
from typing import Any
from collections.abc import Iterator

import xlrd

from rigour.mime.types import XLSX, XLS
import openpyxl

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api

SEBI_DEBARRMENT_URL = "https://nsearchives.nseindia.com/content/press/prs_ra_sebi.xls"
OTHER_DEBARRMENT_URL = (
    "https://nsearchives.nseindia.com/content/press/prs_ra_others.xls"
)

OWNERSHIP_MARKER = re.compile(
    r"\b(?:proprietor(?![a-z])|owner(?![a-z])|prop\.)(?P<of>\s+of\b)?",
    re.IGNORECASE,
)
PROPRIETORY_CONCERN_MARKER = re.compile(
    r"\bproprietory\s+concern\s+of\b", re.IGNORECASE
)
PROPRIETORSHIP_FIRM_MARKER = re.compile(
    r"\s*&\s*its\s+proprietorship\s+firm\s+viz\.\s*", re.IGNORECASE
)
ASSET_RELATIONSHIP_SUFFIX = re.compile(
    r"\s*(?:(?:and|/)\s+(?:the\s+)?its?|\(\s*represented\s+by\s+its)"
    r"(?:\s+sole)?\s*$",
    re.IGNORECASE,
)
PERSON_PREFIX = re.compile(
    r"^(?:(?:mr|mrs|ms|dr)\.(?=\s*\w)|(?:mr|mrs|ms|dr|shri|smt)\.?\s+)",
    re.IGNORECASE,
)
BUSINESS_WORD = re.compile(
    r"\b(?:advisory|advisor|academy|capital|company|consultancy|consultant|"
    r"enterprise|financial|firm|gainer|infotech|institute|investment|market|"
    r"money|organisation|research|securities|services|solutions|system|tips|"
    r"trader|trading)\b",
    re.IGNORECASE,
)


def clean_ownership_name(name: str) -> str:
    """Remove punctuation used to separate an owner from their business."""
    name = re.sub(
        r"\(\s*individual\s+capacity\s+and\s*$", "", name, flags=re.IGNORECASE
    )
    return name.strip(" \t\r\n,;:-–—?()")


def looks_like_person(name: str) -> bool:
    return PERSON_PREFIX.search(name) is not None


def looks_like_business(name: str) -> bool:
    return name.casefold().startswith("m/s") or BUSINESS_WORD.search(name) is not None


def split_ownership_name(name: str) -> tuple[str, str, bool] | None:
    """Split a combined proprietor/business name into owner and asset names."""
    proprietorship_match = PROPRIETORSHIP_FIRM_MARKER.search(name)
    if proprietorship_match is not None:
        owner_name = clean_ownership_name(name[: proprietorship_match.start()])
        asset_name = clean_ownership_name(name[proprietorship_match.end() :])
        if not owner_name or not asset_name:
            return None
        return owner_name, asset_name, True

    proprietory_match = PROPRIETORY_CONCERN_MARKER.search(name)
    matches = list(OWNERSHIP_MARKER.finditer(name))
    if proprietory_match is not None:
        if matches:
            return None
        left = clean_ownership_name(name[: proprietory_match.start()])
        right = clean_ownership_name(name[proprietory_match.end() :])
        if not left or not right:
            return None
        return right, left, False
    if len(matches) != 1:
        return None

    match = matches[0]
    left = clean_ownership_name(name[: match.start()])
    right = clean_ownership_name(name[match.end() :])
    if not left or not right:
        return None

    # "Person, proprietor of Business" is unambiguous. Without "of", use the
    # source's person titles and common business terms to handle both orders.
    person_first = match.group("of") is not None
    if not person_first:
        left_is_person = looks_like_person(left)
        right_is_person = looks_like_person(right)
        left_is_business = looks_like_business(left)
        right_is_business = looks_like_business(right)
        if left_is_person != right_is_person:
            person_first = left_is_person
        elif left_is_business != right_is_business:
            person_first = right_is_business

    owner_name, asset_name = (left, right) if person_first else (right, left)
    relationship_match = ASSET_RELATIONSHIP_SUFFIX.search(asset_name)
    asset_is_debarred = relationship_match is not None
    if relationship_match is not None:
        asset_name = asset_name[: relationship_match.start()]
        asset_name = clean_ownership_name(asset_name)
    return owner_name, asset_name, asset_is_debarred


def load_sheet(workbook: Any, possible_names: list[str]) -> Any:
    for name in possible_names:
        try:
            return workbook[name]
        except KeyError:
            continue
    raise ValueError("None of the worksheet names exist")


def crawl_ownership(
    context: Context, owner: Entity, asset_name: str, is_debarred: bool = False
) -> Entity:
    # The source describes these as proprietorship concerns of the debarred
    # person. Company inherits from both Asset and LegalEntity, so it fits the
    # range of Ownership:asset (LegalEntity does not).
    asset = context.make("Company")
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


def crawl_item(context: Context, input_dict: dict[str, str | None]) -> None:
    name = input_dict.pop("entity_individual_name")
    pan = input_dict.pop("pan", "")
    if name is None:
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, pan)

    asset = None
    debarreds = []

    ownership_names = split_ownership_name(name)
    if ownership_names is not None:
        name, asset_name, asset_is_debarred = ownership_names
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
    din_cin: str = input_dict.pop("din_cin", None) or ""
    if din_cin and "-" not in din_cin:
        entity.add("description", din_cin)
        entity.add("registrationNumber", din_cin.split(" "))

    nse_circular_no = input_dict.pop("nse_circular_no_for_debarment")
    order_date = input_dict.pop("order_date")
    order_particulars = input_dict.pop("order_particulars")
    assert order_particulars is not None
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
            h.apply_date(
                sanction,
                "endDate",
                period,
                # No debarment on the list predates 1990, and debarments can end
                # in the future.
                two_digit_year_base=1990,
            )

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


def parse_xls_or_xlsx_sheet_from_url(
    context: Context, url: str, filename: str
) -> Iterator[dict[str, str | None]]:
    _, _, _, filepath_tmp = zyte_api.fetch_resource(
        context, filename=f"{filename}.temp", url=url, geolocation="in"
    )
    # XLSX is a zipfile internally, sniff for that to detect mimetype
    if zipfile.is_zipfile(filepath_tmp):
        filepath = Path(
            shutil.move(filepath_tmp, context.get_resource_path(f"{filename}.xlsx"))
        )
        mimetype = XLSX
        workbook = openpyxl.load_workbook(filepath)
        # One of the sheets is named "Sheet1" and the other is named "Working" in separate files
        sheet = load_sheet(workbook, ["Sheet1", "Working"])
        items = h.parse_xlsx_sheet(context, sheet, extract_links=True)
    else:
        filepath = Path(
            shutil.move(filepath_tmp, context.get_resource_path(f"{filename}.xls"))
        )
        mimetype = XLS
        items = h.parse_xls_sheet(context, xlrd.open_workbook(str(filepath))["Sheet1"])

    context.export_resource(filepath, mimetype, title=context.SOURCE_TITLE)
    return items


def crawl(context: Context) -> None:
    items: list[dict[str, str | None]] = []

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

        crawl_item(context, item)
