import re
import shutil
import zipfile
from pathlib import Path
from typing import Any
from collections.abc import Iterator

import xlrd

from pydantic import BaseModel, Field
from rigour.mime.types import XLSX, XLS
import openpyxl

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.extract.llm import run_typed_text_prompt
from zavod.stateful.review import (
    TextSourceValue,
    assert_all_accepted,
    review_extraction,
)

SEBI_DEBARRMENT_URL = "https://nsearchives.nseindia.com/content/press/prs_ra_sebi.xls"
OTHER_DEBARRMENT_URL = (
    "https://nsearchives.nseindia.com/content/press/prs_ra_others.xls"
)

OWNERSHIP_MARKER = re.compile(
    r"\b(?:proprietor(?:y|ship)?|owner)\b|\bprop\.", re.IGNORECASE
)
LLM_MODEL_VERSION = "gpt-5.4"


class OwnershipExtraction(BaseModel):
    owner_name: str = Field(
        min_length=1,
        description="Name of the natural or legal person that owns the business.",
    )
    asset_name: str = Field(
        min_length=1,
        description="Name of the proprietorship, trading firm, or business asset.",
    )
    asset_is_debarred: bool = Field(
        description=(
            "Whether the source explicitly includes the business itself among the "
            "debarred parties, rather than only identifying it as the owner's business."
        )
    )


OWNERSHIP_PROMPT = """
Extract an owner and their proprietorship or trading business from the supplied
`Entity / Individual Name` cell of the NSE debarment list.

Rules:
- Return the owner's name in `owner_name` and the business name in `asset_name`,
  regardless of which one appears first.
- Preserve the spelling, capitalization, initials, and honorifics of each name exactly
  as written. Do not correct, expand, or invent names.
- Remove only the prose expressing the ownership relationship and punctuation or
  brackets used solely to enclose that prose.
- Set `asset_is_debarred` to true only if the wording lists both the owner and business
  as subjects, for example "Business and its Proprietor Person" or
  "Person & its proprietorship firm viz. Business".
- Set `asset_is_debarred` to false when the business is only used to identify the
  owner's trade name, for example "Person, Proprietor of Business".
"""


def extract_ownership(
    context: Context,
    entity: Entity,
    name: str,
    source_url: str | None,
) -> tuple[OwnershipExtraction, str | None] | None:
    """Request review of an ownership name and return only accepted extraction."""
    if OWNERSHIP_MARKER.search(name) is None:
        return None

    source_value = TextSourceValue(
        key_parts=name,
        label="Entity / Individual Name ownership extraction",
        text=name,
        url=source_url,
    )
    extraction = run_typed_text_prompt(
        context,
        prompt=OWNERSHIP_PROMPT,
        string=source_value.value_string,
        response_type=OwnershipExtraction,
        model=LLM_MODEL_VERSION,
    )
    review = review_extraction(
        context,
        source_value=source_value,
        original_extraction=extraction,
        origin=LLM_MODEL_VERSION,
    )
    review.link_entity(context, entity)
    if not review.accepted:
        return None
    return review.extracted_data, review.origin


def load_sheet(workbook: Any, possible_names: list[str]) -> Any:
    for name in possible_names:
        try:
            return workbook[name]
        except KeyError:
            continue
    raise ValueError("None of the worksheet names exist")


def crawl_ownership(
    context: Context,
    owner: Entity,
    asset_name: str,
    is_debarred: bool = False,
    origin: str | None = None,
) -> Entity:
    # The source describes these as proprietorship concerns of the debarred
    # person. Company inherits from both Asset and LegalEntity, so it fits the
    # range of Ownership:asset (LegalEntity does not).
    asset = context.make("Company")
    asset.id = context.make_id(owner.id, asset_name)
    asset.add("name", asset_name, origin=origin)
    if is_debarred:
        asset.add("topics", "debarment", origin=origin)
    ownership = context.make("Ownership")
    ownership.id = context.make_id("own", owner.id, asset_name)
    ownership.add("owner", owner, origin=origin)
    ownership.add("asset", asset, origin=origin)
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
    name_origin = None

    ownership_result = extract_ownership(
        context, entity, name, input_dict.get("source_url")
    )
    if ownership_result is not None:
        ownership, name_origin = ownership_result
        name = ownership.owner_name
        asset = crawl_ownership(
            context,
            entity,
            ownership.asset_name,
            is_debarred=ownership.asset_is_debarred,
            origin=name_origin,
        )
        if ownership.asset_is_debarred:
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

    entity.add("name", name, origin=name_origin)
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

    # The raw combined name is retained while an ownership extraction awaits review,
    # so outstanding reviews should not block publication of this daily dataset.
    assert_all_accepted(context, raise_on_unaccepted=False)
