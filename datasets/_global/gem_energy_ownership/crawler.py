import openpyxl
import re
from typing import Dict, Set

from zavod import Context
from zavod import helpers as h

IGNORE = [
    "registration_subdivision",
    "publiclylisted",
    "registration_subdivision",
    "headquarters_subdivision",
    "gem_parents",
    "gem_parents_ids",
]

# Some context, please delete after review

# Unique entity types
# {"person", "unknown entity", "state", "legal entity", "arrangement", "state body"}

# What does 'arrangement' mean?
# A legal arrangement, agreement, contract or other mechanism via which one or more natural
# or legal persons can associate to exert ownership or control over an entity. Parties to an
# arrangement have no other form of collective legal identity.

SKIP_IDS = {
    "E100001015587",  # Small shareholders
    "E100000126067",  # Non-promoter shareholders
    "E100000125842",  # Co-investment by natural persons
    "E100000123261",  # natural persons
}
SELF_OWNED = {"E100000002236"}
STATIC_URL = "https://globalenergymonitor.org/wp-content/uploads/2025/02/Global-Energy-Ownership-Tracker-February-2025.xlsx"
REGEX_URL_SPLIT = re.compile(r",\s*http")


def split_urls(value: str):
    return REGEX_URL_SPLIT.sub("\nhttp", value).split("\n")


def crawl_company(context: Context, row: Dict[str, str], skipped: Set[str]):
    id = row.pop("entity_id")
    name = row.pop("name")
    # Skip entities
    if name is None or id in SKIP_IDS:
        skipped.add(id)
        return
    original_name = row.pop("name_local")
    reg_country = row.pop("registration_country")
    head_country = row.pop("headquarters_country")
    lei_code = row.pop("global_legal_entity_identifier_index")
    entity_type = row.pop("entity_type")
    perm_id = row.pop("permid_refinitiv_permanent_identifier")
    sp_cap = row.pop("s_p_capital_iq")
    uk_id = row.pop("uk_companies_house")
    us_sec_id = row.pop("us_sec_central_index_key")
    us_eia_id = row.pop("us_eia")
    br_id = row.pop(
        "brazil_national_registry_of_legal_entities_federal_revenue_service"
    )
    in_id = row.pop(
        "india_corporate_identification_number_ministry_of_corporate_affairs"
    )
    ru_id = row.pop(
        "russia_uniform_state_register_of_legal_entities_of_russian_federation"
    )

    if entity_type == "legal entity":
        schema = "Company"
    elif entity_type == "state body" or entity_type == "state":
        schema = "PublicBody"
    elif entity_type == "person":
        schema = "Person"
    else:
        schema = "Company"

    entity = context.make(schema)
    entity.id = context.make_slug(id)

    entity.add("name", name)
    entity.add("alias", row.pop("full_name"))
    entity.add("name", original_name)
    entity.add("alias", row.pop("name_other"))
    entity.add("weakAlias", row.pop("abbreviation"))
    if lei_code != "not found":
        entity.add("leiCode", lei_code)
    if entity_type != "unknown entity":
        entity.add("description", entity_type)
    entity.add("classification", row.pop("legal_entity_type"))
    entity.add("country", reg_country)
    entity.add("mainCountry", head_country)
    homepage = row.pop("home_page")
    if homepage:
        entity.add("website", split_urls(homepage))
    if schema != "Person":
        entity.add("permId", perm_id)
        # find a way to remap invalid ones
        entity.add("ogrnCode", ru_id)
        entity.add("registrationNumber", br_id)
        entity.add("registrationNumber", uk_id)
        entity.add("registrationNumber", in_id)
        entity.add("registrationNumber", us_eia_id)
        entity.add("registrationNumber", sp_cap)
        if schema != "PublicBody":
            entity.add("cikCode", us_sec_id)
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

    context.audit_data(row, ignore=["subject_entity_name"])
    context.emit(ownership)


def crawl(context: Context):
    path = context.fetch_resource("source.xlsx", STATIC_URL)
    # context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    workbook: openpyxl.Workbook = openpyxl.load_workbook(path, read_only=True)
    skipped: Set[str] = set()

    for row in h.parse_xlsx_sheet(context, sheet=workbook["All Entities"]):
        crawl_company(context, row, skipped)
    for row in h.parse_xlsx_sheet(context, sheet=workbook["Entity Ownership"]):
        crawl_rel(context, row, skipped)
