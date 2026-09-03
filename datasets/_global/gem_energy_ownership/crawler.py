import openpyxl
import re

from zavod import Context
from zavod import helpers as h
from zavod.shed.internal_data import fetch_internal_data

# Unique entity types
# {"person", "unknown entity", "state", "legal entity", "arrangement", "state body"}

IGNORE = [
    "registration_subdivision",
    "publiclylisted",
    "registration_subdivision",
    "headquarters_subdivision",
    "gem_parents",
    "gem_parents_ids",
    "intermediate_owner_ids",
    "joint_venture",
    "entity_status_data_source_url",
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
REGEX_URL_SPLIT = re.compile(r",\s*http")
REGEX_ENTITY_ID = re.compile(r"^E\d+$")
REGEX_POSSIBLE_ASSOCIATES = re.compile(r"（[^（）]*、[^（）]*）| \(\s*[^()]*,[^()]*\)")


def clean_entity_id(value: str) -> str | None:
    """Normalise an entity reference, or None if it isn't one.

    Some references are written as floats, e.g. "E100001014363.0".
    """
    ident = value.strip().removesuffix(".0")
    return ident if REGEX_ENTITY_ID.match(ident) else None


def split_urls(value: str) -> list[str]:
    # Some cells are CSV fragments where the first URL is still quoted.
    parts = REGEX_URL_SPLIT.sub("\nhttp", value).split("\n")
    return [url for url in (part.strip().strip('"') for part in parts) if url]


def split_associates(
    context: Context, name: str
) -> tuple[str, str, set[tuple[str, str]]]:
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


def crawl_company(
    context: Context,
    row: dict[str, str | None],
    skipped: set[str],
    owned_ids: set[str],
    entity_ids: set[str],
) -> None:
    id_ = row.pop("entity_id")
    if id_ is None:
        context.log.warning("Missing entity ID", row=row)
        return
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
    elif entity_type is None or entity_type == "unknown entity":
        schema = "LegalEntity"
    else:
        context.log.warning("Unknown entity type", entity_type=entity_type)
        return

    # An owned entity is the `asset` of an Ownership, so it has to be an Asset.
    # Company is the only schema used here that is both a LegalEntity and an Asset.
    if id_ in owned_ids and schema in ("Organization", "LegalEntity"):
        schema = "Company"

    entity = context.make(schema)
    entity.id = context.make_slug(id_)

    original_names = [
        row.pop("name"),
        row.pop("full_name"),
        row.pop("name_local"),
    ]
    # (potentially trimmed name, original string)
    associates: set[tuple[str, str]] = set()
    names: set[tuple[str, str]] = set()
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
    # "dissolved" or "amalgamated"; absent for entities still trading.
    entity.add("status", row.pop("entity_status"))
    entity.add("country", reg_country)
    entity.add("mainCountry", headquarters_country)
    homepage = row.pop("home_page")
    if homepage:
        entity.add("website", split_urls(homepage))
    if not entity.schema.is_a("Person"):
        br_id = row.pop(
            "brazil_national_registry_of_legal_entities_federal_revenue_service"
        )
        entity.add("registrationNumber", br_id)
        in_id = row.pop(
            "india_corporate_identification_number_ministry_of_corporate_affairs",
        )
        entity.add("registrationNumber", in_id)
        if perm_id != "not found":
            entity.add_cast("Company", "permId", perm_id)
        ru_id = row.pop(
            "russia_uniform_state_register_of_legal_entities_of_russian_federation",
        )
        entity.add("ogrnCode", ru_id)
        entity.add("registrationNumber", row.pop("uk_companies_house"))
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

    # Entities marked "amalgamated" name the entity they merged into. About a third
    # of those targets are not published anywhere in the workbook, so the succession
    # can only be recorded when the successor is an entity we actually emit.
    merged_into = row.pop("merged_into")
    if merged_into is not None:
        successor_id = clean_entity_id(merged_into)
        if successor_id is None:
            context.log.warning(
                "Malformed merged_into value",
                entity_id=id_,
                merged_into=merged_into,
            )
        elif successor_id not in entity_ids:
            context.log.info(
                "Skipping merger into an entity the source doesn't publish",
                entity_id=id_,
                merged_into=successor_id,
            )
        elif successor_id not in SKIP_IDS:
            succession = context.make("Succession")
            succession.id = context.make_id(id_, successor_id)
            succession.add("predecessor", entity)
            succession.add("successor", context.make_slug(successor_id))
            context.emit(succession)

    context.audit_data(
        row,
        ignore=IGNORE,
    )


def crawl_rel(context: Context, row: dict[str, str | None], skipped: set[str]) -> None:
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
    ownership.add("percentage", f"{float(percentage):.2f}" if percentage else None)
    source_urls = row.pop("data_source_url")
    if source_urls is not None:
        ownership.add("sourceUrl", split_urls(source_urls))

    context.audit_data(
        row, ignore=["subject_entity_name", "interested_party_name", "share_imputed"]
    )
    context.emit(ownership)


def crawl(context: Context) -> None:
    path = context.get_resource_path("source.xlsx")
    fetch_internal_data(
        "gem_energy_ownership/Global-Energy-Ownership-Tracker-August-2026-V2.xlsx",
        path,
    )
    workbook: openpyxl.Workbook = openpyxl.load_workbook(path, read_only=True)
    skipped: set[str] = set()

    ownership_rows = list(
        h.parse_xlsx_sheet(context, sheet=workbook["Entity Ownership"])
    )
    owned_ids = {
        subject_id
        for row in ownership_rows
        if (subject_id := row["subject_entity_id"]) is not None
    }

    entity_rows = list(h.parse_xlsx_sheet(context, sheet=workbook["All Entities"]))
    entity_ids = {
        entity_id for row in entity_rows if (entity_id := row["entity_id"]) is not None
    }

    for row in entity_rows:
        crawl_company(context, row, skipped, owned_ids, entity_ids)
    for row in ownership_rows:
        crawl_rel(context, row, skipped)
