import gzip
import csv
from typing import Any, List, Dict
from itertools import chain

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise, OccupancyStatus
from rigour.mime.types import GZIP


BASE_API = "https://data.brreg.no/enhetsregisteret/api"

# incorporation & dissolution dates to check
INC_DATES = [
    "registration_date_entity_register",
    "foundation_date",
    "registration_date_business_register",
]
DISSOLV_DATES = [
    "bankruptcy_date",
    "under_liquidation_date",
    "forced_dissolution_missing_ceo_date",
    "forced_dissolution_missing_auditor_date",
    "forced_dissolution_missing_accounts_date",
    "forced_dissolution_deficient_board_date",
    "forced_liquidation_missing_deletion_date",
]
# should we put this there ^: under_foreign_insolvency_proceedings_date ?


def get_oldest_date(dict_row: Dict[str, Any], keys_to_check: List[str]) -> str | None:
    """
    Get the oldest date value from keys to check, delete the rest.
    """
    values = []
    for k in keys_to_check:
        v = dict_row.pop(k, None)
        values.append(v)
    return min(values) if values else None


def crawl_row(context: Context, dict_row: Dict[Any, Any]) -> None:
    # === get state-owned enterpreses details === #
    company_name = dict_row.pop("name")
    org_number = dict_row.pop("org_number")
    company_juris = dict_row.pop("subject_to_legislation_country_code", None)

    entity = context.make("LegalEntity")
    entity.id = context.make_id(company_name, org_number)
    entity.add("name", company_name)
    entity.add("jurisdiction", company_juris)
    entity.add("legalForm", dict_row.pop("org_form_description"))
    # entity.add("description", dict_row.pop("statutory_purpose", None))
    # entity.add("notes", dict_row.pop("activity", None))

    foreign_register_name = dict_row.pop("foreign_register_name", None)
    if foreign_register_name is not None:
        entity.add("alias", foreign_register_name)

    # bankrupcy or liquidation bools
    is_bankrupt = dict_row.pop("bankruptcy", None) == "true"
    is_liquidated = (
        dict_row.pop("under_forced_liquidation_or_dissolution", None) == "true"
    )
    if is_bankrupt:
        entity.add("status", "konkurs")
    if is_liquidated:
        entity.add("status", "underTvangsavviklingEllerTvangsopplosning")

    # registrationNumber
    entity.add(
        "registrationNumber", dict_row.pop("registration_number_home_country", None)
    )
    # about industry codes: https://www.brreg.no/bedrift/naeringskoder/
    industry_codes = {
        k: v
        for k, v in dict_row.items()
        if k.startswith("industry_code") and k.endswith("code")
    }
    for k, v in industry_codes.items():
        entity.add("sector", v)
        dict_row.pop(k)

    # # === contacts === #
    # entity.add("email", dict_row.pop("email"))
    # website = dict_row.pop("website", None)
    # if website and (
    #     website.isdigit()
    #     or any(word in website.lower() for word in ("instagram", "facebook", "google"))
    # ):
    #     website = None
    # entity.add("website", website)
    # entity.add("phone", dict_row.pop("phone", None))
    # entity.add("phone", dict_row.pop("phone_mobile", None))
    # h.apply_date(entity, "incorporationDate", get_oldest_date(dict_row, INC_DATES))
    # h.apply_date(entity, "dissolutionDate", get_oldest_date(dict_row, DISSOLV_DATES))

    # # === addresses === #
    # ba_country = dict_row.pop("business_address_country_code", None) or ""
    # country_code_business = ba_country.lower()
    # if country_code_business not in entity.countries:
    #     entity.add("country", country_code_business)
    # business_address = h.make_address(
    #     context,
    #     street=dict_row.pop("business_address_street", None),
    #     city=dict_row.pop("business_address_city", None),
    #     postal_code=dict_row.pop("business_address_postal_code", None),
    #     country_code=country_code_business,
    # )
    # h.copy_address(entity, business_address)

    # pa_country = dict_row.pop("postal_address_country_code", None) or ""
    # country_code_postal = pa_country.lower()
    # if country_code_postal not in entity.countries:
    #     entity.add("country", country_code_postal)
    # postal_address = h.make_address(
    #     context,
    #     street=dict_row.pop("postal_address_street", None),
    #     city=dict_row.pop("postal_address_city", None),
    #     postal_code=dict_row.pop("postal_address_postal_code", None),
    #     country_code=country_code_postal,
    # )
    # h.copy_address(entity, postal_address)
    # foreign_address = h.make_address(
    #     context,
    #     street=dict_row.pop("foreign_register_address_street", None),
    #     city=dict_row.pop("foreign_register_address_city", None),
    #     country=dict_row.pop("foreign_register_address_country", None),
    # )
    # h.copy_address(entity, foreign_address)

    # get legal entity's parent ID
    parent_id = dict_row.pop("parent_entity_org_number")
    if parent_id is not None and parent_id != "":
        owner = context.make("LegalEntity")
        owner.id = context.make_id(parent_id)

        ownership = context.make("Ownership")
        ownership.id = context.make_id("ownership", entity.id, owner.id)
        ownership.add("owner", owner)
        ownership.add("asset", entity)
        context.emit(ownership)

    # === fetch peps === #
    API_org_url = f"{BASE_API}/enheter/{org_number}/roller"
    rollegrupper = context.fetch_json(API_org_url)["rollegrupper"]
    # all positions listed here: https://data.brreg.no/enhetsregisteret/api/roller/rollegruppetyper
    # DAGL: Managing Director / CEO
    # BEST: Managing Shipowner
    # INNH: Sole Proprietor / Owner
    # FFØR: Business Manager
    # Styre: Board of Directors

    # flatten to iterate over "rollegrupper" that contains a list of roles
    roles = chain.from_iterable(group.get("roller") for group in rollegrupper)
    for role in roles:
        role_data = role.get("type")
        role_code = role_data.get("kode")

        # get board and C-level positions
        role_codes_to_fetch = ["Styre", "DAGL", "BEST", "INNH", "FFØR"]
        if role_code in role_codes_to_fetch:
            role_name = role_data.get("beskrivelse")

            person_data = role.get("person")
            if person_data is None:
                continue
            names = person_data.get("navn")
            first_name = names.pop("fornavn")
            last_name = names.pop("etternavn")

            pep = context.make("Person")
            pep.id = context.make_id(first_name, last_name)
            pep.add("birthDate", person_data.pop("fodselsdato"))
            pep.add("firstName", first_name)
            pep.add("lastName", last_name)
            pep.add("country", "no")

            position = h.make_position(
                context,
                name=f"{role_name}, {company_name}",
                topics=["gov.soe"],
                country=[
                    "no",
                    company_juris,
                ],  # adding company's country of jurisdiction, too
                organization=entity,
            )
            categorisation = categorise(context, position, is_pep=True)
            occupancy = h.make_occupancy(
                context,
                pep,
                position,
                False,
                categorisation=categorisation,
                status=OccupancyStatus.UNKNOWN,
            )

            if occupancy is not None:
                context.emit(pep)
                context.emit(entity)
                context.emit(position)
                context.emit(occupancy)

    # COLS_NOT_IN_USE_SUBSTR = (
    #     "employee_count",
    #     "municipality",
    #     "vat_register",
    #     "org_register",
    #     "business_register",
    #     "foundation_register",
    #     "party_register",
    #     "opt_out",
    #     "capital",
    #     "subject_to_legislation",
    #     "company_form_home",
    # )
    # cols_matched_by_substr = [
    #     k for k in dict_row if any(substr in k for substr in COLS_NOT_IN_USE_SUBSTR)
    # ]
    # cols_industry_code_descriptions = [
    #     k
    #     for k in dict_row
    #     if k.startswith("industry_code") and k.endswith("description")
    # ]
    # cols_not_in_use_exact = [
    #     "org_form_code",
    #     "support_unit_code",
    #     "support_unit_code_description",
    #     "institutional_sector_code",
    #     "last_submitted_annual_accounts",
    #     "under_liquidation",
    #     "language_form",
    #     "articles_of_association_date",
    #     "statutory_purpose",
    #     "activity",
    #     "endorsements",
    #     "under_foreign_insolvency_proceedings_date",
    #     "under_reconstruction_negotiations_date",
    #     "is_in_corporate_group",
    #     "business_address_country",  # use postal codes instead
    #     "postal_address_country",  # use postal codes instead
    # ]
    # COLS_NOT_IN_USE = (
    #     cols_matched_by_substr + cols_industry_code_descriptions + cols_not_in_use_exact
    # )
    # context.audit_data(dict_row, ignore=COLS_NOT_IN_USE)


def crawl(context: Context) -> None:
    page = context.fetch_html(
        context.data_url,
        method="GET",
        absolute_links=True,
    )
    link_element = h.xpath_element(
        page, "//a[contains(@href, '/enhetsregisteret/api/enheter/lastned/csv')]"
    )
    url = link_element.get("href")
    assert url is not None, "Could not find CSV download link"

    path = context.fetch_resource("source.csv.gz", url)
    context.export_resource(path, GZIP, title=context.SOURCE_TITLE)

    # read the norwegian header
    with gzip.open(path, "rt") as f:
        reader = csv.reader(f)
        nok_header_row = next(reader)

        # translate to english headers
        eng_header_row = [
            context.lookup_value("headers", header, warn_unmatched=True)
            for header in nok_header_row
        ]

        # iterate over the rows with english field names
        dict_reader = csv.DictReader(f, fieldnames=eng_header_row)
        for row in dict_reader:
            row = {k: v.strip() for k, v in row.items() if k is not None}

            # collect only state-owned enterprises
            institutional_sector_description = row.pop(
                "institutional_sector_description", None
            )
            if (
                institutional_sector_description is not None
                and "Statlig eide" in institutional_sector_description
            ):
                crawl_row(context, row)
