import openpyxl
import re
from typing import Dict, Set

from zavod import Context
from zavod import helpers as h

IGNORE = [
    "sequence_no",
    "us_eia_860",
    "parent_entity_ids",
    "parents",
    "headquarters_country",
    "publicly_listed",
    "decision_date",
    "gcpt_announced_mw",
    "gcpt_cancelled_mw",
    "gcpt_construction_mw",
    "gcpt_mothballed_mw",
    "gcpt_operating_mw",
    "gcpt_permitted_mw",
    "gcpt_pre_permit_mw",
    "gcpt_retired_mw",
    "gcpt_shelved_mw",
    "gogpt_announced_mw",
    "gogpt_cancelled_mw",
    "gogpt_construction_mw",
    "gogpt_mothballed_mw",
    "gogpt_operating_mw",
    "gogpt_pre_construction_mw",
    "gogpt_retired_mw",
    "gogpt_shelved_mw",
    "gbpt_announced_mw",
    "gbpt_construction_mw",
    "gbpt_mothballed_mw",
    "gbpt_operating_mw",
    "gbpt_pre_construction_mw",
    "gbpt_retired_mw",
    "gbpt_shelved_mw",
    "gbpt_cancelled_mw",
    "gcmt_proposed_mtpa",
    "gcmt_operating_mtpa",
    "gcmt_shelved_mtpa",
    "gcmt_mothballed_mtpa",
    "gcmt_cancelled_mtpa",
    "gspt_operating_ttpa",
    "gspt_announced_ttpa",
    "gspt_construction_ttpa",
    "gspt_retired_ttpa",
    "gspt_operating_pre_retirement_ttpa",
    "gspt_mothballed_ttpa",
    "gspt_cancelled_ttpa",
    "total",
]
SKIP_IDS = {
    "E100001015587",  # Small shareholders
    "E100000132388",  # Unknown
    "E100000001753",  # Other
    "E100000126067",  # Non-promoter shareholders
    "E100000125842",  # Co-investment by natural persons
    "E100000123261",  # natural persons
}
SELF_OWNED = {"E100000002239"}
STATIC_URL = "https://data.opensanctions.org/contrib/globalenergy/Global_Energy_Ownership_Tracker_June_2024.xlsx"
REGEX_URL_SPLIT = re.compile(r",\s*http")


def split_urls(value: str):
    return REGEX_URL_SPLIT.sub("\nhttp", value).split("\n")


def crawl_company(context: Context, row: Dict[str, str], skipped: Set[str]):
    id = row.pop("entity_id")
    name = row.pop("entity_name")
    # Skip entities
    if name is None or id in SKIP_IDS:
        skipped.add(id)
        return
    original_name = row.pop("name_local")
    reg_country = row.pop("registration_country")
    lei_code = row.pop("legal_entity_identifier")
    entity_type = row.pop("entity_type")

    if entity_type == "legal entity":
        schema = "Company"
    elif entity_type == "state body" or entity_type == "state":
        schema = "PublicBody"
    elif entity_type == "person":
        schema = "Person"
    else:
        schema = "Company"  # 3 universities end up being companies

    entity = context.make(schema)
    entity.id = context.make_slug(id)

    entity.add("name", name)
    entity.add("name", original_name)
    entity.add("alias", row.pop("name_other"))
    entity.add("weakAlias", row.pop("abbreviation"))
    if lei_code != "unknown":
        entity.add("leiCode", lei_code)
    if entity_type != "unknown entity":
        entity.add("description", entity_type)
    entity.add("country", reg_country)
    homepage = row.pop("home_page")
    if homepage:
        entity.add("website", split_urls(homepage))
    if schema != "Person":
        entity.add("permId", row.pop("refinitiv_permid"))
        if schema != "PublicBody":
            entity.add("cikCode", row.pop("sec_central_index_key"))
    address = h.format_address(
        country=reg_country,
        state=row.pop("registration_subdivision"),
        city=row.pop("headquarters_subdivision"),
    )
    entity.add("address", address)

    context.emit(entity)
    context.audit_data(
        row,
        ignore=IGNORE,
    )


def crawl_rel(context: Context, row: Dict[str, str], skipped: Set[str]):
    subject_entity_id = row.pop("subject_entity_id")
    interested_party_id = row.pop("interested_party_id")
    interested_party_name = row.pop("interested_party_name")

    # Skip the relationship if either ID is in the skipped set
    if subject_entity_id in skipped or interested_party_id in skipped:
        return

    if subject_entity_id == interested_party_id and subject_entity_id in SELF_OWNED:
        return
    entity = context.make("LegalEntity")
    entity.id = context.make_slug(interested_party_id)
    entity.add("name", interested_party_name)
    context.emit(entity)

    ownership = context.make("Ownership")
    ownership.id = context.make_id(subject_entity_id, interested_party_id)
    ownership.add("asset", context.make_slug(subject_entity_id))
    ownership.add("owner", context.make_slug(interested_party_id))
    percentage = row.pop("share_of_ownership")
    ownership.add("percentage", "%.2f" % float(percentage) if percentage else None)
    source_urls = row.pop("data_source_url")
    if source_urls is not None:
        ownership.add("sourceUrl", split_urls(source_urls))

    context.audit_data(
        row, ignore=["subject_entity_name", "interested_party_name", "index"]
    )
    context.emit(ownership)


def crawl(context: Context):
    path = context.fetch_resource("source.xlsx", STATIC_URL)
    # context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    workbook: openpyxl.Workbook = openpyxl.load_workbook(path, read_only=True)
    skipped: Set[str] = set()

    for row in h.parse_xlsx_sheet(context, sheet=workbook["Immediate Owner Entities"]):
        crawl_company(context, row, skipped)
    for row in h.parse_xlsx_sheet(context, sheet=workbook["Parent Entities"]):
        crawl_company(context, row, skipped)
    for row in h.parse_xlsx_sheet(context, sheet=workbook["Entity Relationships"]):
        crawl_rel(context, row, skipped)
