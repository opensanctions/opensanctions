import openpyxl
import re
from typing import Dict, Set

from zavod import Context
from zavod import helpers as h


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
ALIAS_SPLITS = ["[former],", "[former]", "[former name]", "(former)"]
SKIP_IDS = {
    "E100001015587",  # Small shareholders
    "E100000126067",  # Non-promoter shareholders
    "E100000125842",  # Co-investment by natural persons
    "E100000123261",  # natural persons
}
SELF_OWNED = {"E100000002236"}
STATIC_URL = "https://globalenergymonitor.org/wp-content/uploads/2025/02/Global-Energy-Ownership-Tracker-February-2025.xlsx"
REGEX_URL_SPLIT = re.compile(r",\s*http")
PATTERN = r"\(\s*[^()]*,[^()]*\)"


def split_urls(value: str):
    return REGEX_URL_SPLIT.sub("\nhttp", value).split("\n")


def get_associates(
    context: Context,
    name,
    associates,
    full_name,
    original_name,
):
    result = context.lookup("associates", name)
    if result and result.associates:
        for associate_data in result.associates:
            # Update associates
            associates_names = associate_data.get("associates_names", [])
            if associates_names:
                associates.update(associates_names)
            # Overwrite names based on which one was matched
            entity_name = associate_data.get("entity")
            if entity_name:
                if name == full_name:
                    full_name = entity_name
                else:
                    name = original_name
                    original_name = entity_name
    return full_name, original_name, associates


def crawl_company(context: Context, row: Dict[str, str], skipped: Set[str]):
    id = row.pop("entity_id")
    name = row.pop("name")
    full_name = row.pop("full_name")
    aliases = row.pop("name_other")
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
    elif entity_type == "arrangement":
        schema = "LegalEntity"
    elif entity_type == "state body" or entity_type == "state":
        schema = "PublicBody"
    elif entity_type == "person":
        schema = "Person"
    else:
        schema = "Company"

    entity = context.make(schema)
    entity.id = context.make_slug(id)

    # if re.search(PATTERN, name):
    #     context.log.warning(f"Potential candidate for associates: {name}")

    associates: Set[str] = set()
    for name in [full_name, original_name]:
        full_name, original_name, associates = get_associates(
            context, name, associates, full_name, original_name
        )

    if associates:
        for associate in associates:
            other = context.make("LegalEntity")
            other.id = context.make_slug("named", associate)
            other.add("name", associate)
            context.emit(other)

            link = context.make("UnknownLink")
            link.id = context.make_id(entity.id, other.id)
            link.add("subject", entity)
            link.add("object", other)
            context.emit(link)

    entity.add("name", name)
    entity.add("name", full_name)
    entity.add("name", original_name)
    if aliases is not None:
        for alias in h.multi_split(aliases, ALIAS_SPLITS):
            entity.add("previousName", alias)
    entity.add("weakAlias", row.pop("abbreviation"))
    if lei_code != "not found":
        entity.add("leiCode", lei_code)
    if entity_type != "unknown entity":
        entity.add("description", entity_type)
    entity.add("legalForm", row.pop("legal_entity_type"))
    entity.add("country", reg_country)
    entity.add("mainCountry", head_country)
    homepage = row.pop("home_page")
    if homepage:
        entity.add("website", split_urls(homepage))
    if schema != "Person":
        entity.add_cast("Company", "permId", perm_id)
        entity.add("ogrnCode", ru_id)
        entity.add("registrationNumber", br_id)
        entity.add("registrationNumber", uk_id)
        entity.add("registrationNumber", in_id)
        entity.add("registrationNumber", us_eia_id)
        entity.add("registrationNumber", sp_cap)
        if schema != "PublicBody":
            entity.add_cast("Company", "cikCode", us_sec_id)
        if entity.schema.is_a("PublicBody"):
            entity.add("registrationNumber", us_sec_id)
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
    path = context.fetch_resource("source.xlsx", STATIC_URL)
    # context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    workbook: openpyxl.Workbook = openpyxl.load_workbook(path, read_only=True)
    skipped: Set[str] = set()

    for row in h.parse_xlsx_sheet(context, sheet=workbook["All Entities"]):
        crawl_company(context, row, skipped)
    for row in h.parse_xlsx_sheet(context, sheet=workbook["Entity Ownership"]):
        crawl_rel(context, row, skipped)
