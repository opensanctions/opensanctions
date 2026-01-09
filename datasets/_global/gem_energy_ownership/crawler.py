import openpyxl
import re
from typing import Dict, Set, Tuple
from rigour.mime.types import XLSX

from zavod import Context
from zavod import helpers as h
from zavod.extract import zyte_api


# Unique entity types
# {"person", "unknown entity", "state", "legal entity", "arrangement", "state body"}

IGNORE = [
    "registration_subdivision",
    "publiclylisted",
    "registration_subdivision",
    "headquarters_subdivision",
    "gem_parents",
    "gem_parents_ids",
]
ALIAS_SPLITS = [
    "[former],",
    "[former]",
    "[FORMER]",
    "[former name]",
    "[Former]",
    "(former)",
    "[Former}",
    "[former[",
    "; ",
]
SKIP_IDS = {
    "E100001015587",  # Small shareholders
    "E100000126067",  # Non-promoter shareholders
    "E100000125842",  # Co-investment by natural persons
    "E100000123261",  # natural persons
    "E100002001974",  # member/employee owned
}
SELF_OWNED = {"E100000002236"}
STATIC_URL = "https://globalenergymonitor.org/wp-content/uploads/2025/10/Global-Energy-Ownership-Tracker-October-2025-V1.xlsx"
REGEX_URL_SPLIT = re.compile(r",\s*http")
REGEX_POSSIBLE_ASSOCIATES = re.compile(r"（[^（）]*、[^（）]*）| \(\s*[^()]*,[^()]*\)")


def split_urls(value: str):
    return REGEX_URL_SPLIT.sub("\nhttp", value).split("\n")


def split_associates(context: Context, name):
    if REGEX_POSSIBLE_ASSOCIATES.search(name):
        result = context.lookup("associates", name)
        if result is None:
            context.log.warning(f"Potential candidate for associates: {name}")
        else:
            associates = set()
            for associate in result.associates_names:
                associates.add((associate, name))
            return result.entity, name, associates
    return name, name, set()


def crawl_company(context: Context, row: Dict[str, str], skipped: Set[str]):
    id_ = row.pop("entity_id")
    # Skip entities
    if id_ in SKIP_IDS:
        skipped.add(id_)
        return
    reg_country = row.pop("registration_country")
    headquarters_country = row.pop("headquarters_country")
    entity_type = row.pop("entity_type")
    perm_id = row.pop("permid_refinitiv_permanent_identifier")
    topics = None
    if entity_type == "legal entity":
        schema = "Company"
    elif entity_type == "arrangement":
        schema = "LegalEntity"
    elif entity_type == "state body" or entity_type == "state":
        schema = "Organization"
        topics = "gov.soe"
    elif entity_type == "person":
        schema = "Person"
    else:
        schema = "Company"

    entity = context.make(schema)
    entity.id = context.make_slug(id_)

    original_names = [
        row.pop("name"),
        row.pop("full_name"),
        row.pop("name_local"),
    ]
    if not any(original_names):
        names = [id_]
    # (potentially trimmed name, original string)
    associates: Set[Tuple[str, str]] = set()
    names: Set[Tuple[str, str]] = set()
    for name in original_names:
        if name is None:
            continue
        name, orig_name, associates_ = split_associates(context, name)
        names.add((name, orig_name))
        associates.update(associates_)

    if associates:
        for associate, orig_name in associates:
            other = context.make("LegalEntity")
            other.id = context.make_slug("named", associate)
            other.add("name", associate, original_value=orig_name)
            other.add("country", headquarters_country)
            context.emit(other)

            link = context.make("UnknownLink")
            link.id = context.make_id(entity.id, other.id)
            link.add("subject", entity)
            link.add("object", other)
            context.emit(link)

    for name, orig_name in names:
        entity.add("name", name, original_value=orig_name)
    aliases = row.pop("name_other")
    if aliases is not None:
        for alias in h.multi_split(aliases, ALIAS_SPLITS):
            entity.add("alias", alias)
    entity.add("weakAlias", row.pop("abbreviation"))
    if (lei_code := row.pop("global_legal_entity_identifier_index")) != "not found":
        entity.add("leiCode", lei_code)
    if entity_type != "unknown entity":
        entity.add("description", entity_type)
    entity.add("legalForm", row.pop("legal_entity_type"))
    entity.add("country", reg_country)
    entity.add("mainCountry", headquarters_country)
    homepage = row.pop("home_page")
    if homepage:
        entity.add("website", split_urls(homepage))
    if not entity.schema.is_a("Person"):
        if perm_id != "not found":
            entity.add_cast("Company", "permId", perm_id)
        ru_id = row.pop(
            "russia_uniform_state_register_of_legal_entities_of_russian_federation"
        )
        entity.add("ogrnCode", ru_id)
        br_id = row.pop(
            "brazil_national_registry_of_legal_entities_federal_revenue_service"
        )
        entity.add("registrationNumber", br_id)
        entity.add("registrationNumber", row.pop("uk_companies_house"))
        in_id = row.pop(
            "india_corporate_identification_number_ministry_of_corporate_affairs"
        )
        entity.add("registrationNumber", in_id)
        entity.add("registrationNumber", row.pop("us_eia"))
        entity.add("registrationNumber", row.pop("s_p_capital_iq"))
        if entity.schema.is_a("Organization") and topics is not None:
            entity.add("topics", "gov.soe")
            entity.add("registrationNumber", row.pop("us_sec_central_index_key"))
        else:
            entity.add_cast("Company", "cikCode", row.pop("us_sec_central_index_key"))
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

    # Skip the relationship if either ID is in the skipped set
    if subject_entity_id in skipped or interested_party_id in skipped:
        return

    if subject_entity_id == interested_party_id and subject_entity_id in SELF_OWNED:
        return
    entity = context.make("LegalEntity")
    entity.id = context.make_slug(interested_party_id)

    ownership = context.make("Ownership")
    ownership.id = context.make_id(subject_entity_id, interested_party_id)
    ownership.add("asset", context.make_slug(subject_entity_id))
    ownership.add("owner", context.make_slug(interested_party_id))
    percentage = row.pop("share_of_ownership")
    ownership.add("percentage", "%.2f" % float(percentage) if percentage else None)
    source_urls = row.pop("data_source_url")
    if source_urls is not None:
        ownership.add("sourceUrl", split_urls(source_urls))

    context.audit_data(row, ignore=["subject_entity_name", "interested_party_name"])
    context.emit(ownership)


def crawl(context: Context):
    _, _, _, path = zyte_api.fetch_resource(context, "source.xlsx", STATIC_URL, XLSX)
    # context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    workbook: openpyxl.Workbook = openpyxl.load_workbook(path, read_only=True)
    skipped: Set[str] = set()

    for row in h.parse_xlsx_sheet(context, sheet=workbook["All Entities"]):
        crawl_company(context, row, skipped)
    for row in h.parse_xlsx_sheet(context, sheet=workbook["Entity Ownership"]):
        crawl_rel(context, row, skipped)
