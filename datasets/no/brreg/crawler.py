import gzip
import csv

from rigour.mime.types import GZIP
from typing import Any, List, Dict

from zavod import Context, helpers as h, Entity
from zavod.stateful.positions import categorise, OccupancyStatus


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

# all positions listed here: https://data.brreg.no/enhetsregisteret/api/roller/rollegruppetyper
# DAGL: Managing Director / CEO
# BEST: Managing Shipowner
# INNH: Sole Proprietor / Owner
# FFØR: Business Manager
# STYRE: Board of Directors
# get board and C-level positions
GROUP_CODES = ["STYR", "DAGL", "BEST", "INNH", "FFØR"]
IGNORE = [
    "activity",
    "articles_of_association_date",
    "bankruptcy",
    "business_address_country",
    "business_address_municipality_code",
    "capital_amount",
    "capital_currency",
    "capital_fully_paid_in",
    "capital_introduced_date",
    "capital_paid_in",
    "capital_restricted",
    "capital_share_count",
    "capital_type",
    "email",
    "employee_count",
    "endorsements",
    "foreign_register_address_city",
    "foreign_register_address_country",
    "foreign_register_address_street",
    "has_registered_employee_count",
    "industry_code1_description",
    "industry_code2_description",
    "industry_code3_description",
    "institutional_sector_code",
    "is_in_corporate_group",
    "language_form",
    "last_submitted_annual_accounts",
    "opt_out_audit_date",
    "opt_out_audit_decision_date",
    "org_form_code",
    "parent_entity_org_number",
    "phone_mobile",
    "phone",
    "postal_address_country",
    "postal_address_municipality_code",
    "registered_in_business_register",
    "registered_in_foundation_register",
    "registered_in_party_register",
    "registered_in_vat_register",
    "registered_in_voluntary_org_register",
    "registration_date_employee_count_entity_register",
    "registration_date_employee_count_nav_employment_register",
    "registration_date_vat_register_entity_register",
    "registration_date_vat_register",
    "registration_date_voluntary_vat_register",
    "statutory_purpose",
    "support_unit_code_description",
    "support_unit_code",
    "under_forced_liquidation_or_dissolution",
    "under_forced_liquidation_or_dissolution",
    "under_liquidation",
    "voluntary_vat_registered_description",
    "website",
    "under_foreign_insolvency_proceedings_date",
]


def get_oldest_date(row: Dict[str, Any], keys_to_check: List[str]) -> str | None:
    """
    Get the oldest date value from keys to check, delete the rest.
    """
    values = []
    for k in keys_to_check:
        v = row.pop(k, None)
        values.append(v)
    return min(values) if values else None


def crawl_company(
    context: Context,
    row: Dict[Any, Any],
    company_name: str,
    org_number: str,
    company_juris: str,
) -> Entity:
    entity = context.make("LegalEntity")
    entity.id = context.make_id(company_name, org_number)
    entity.add("name", company_name)
    entity.add("alias", row.pop("foreign_register_name"))
    entity.add("jurisdiction", company_juris)
    entity.add("legalForm", row.pop("org_form_description"))
    entity.add("registrationNumber", row.pop("registration_number_home_country"))
    # about industry codes: https://www.brreg.no/bedrift/naeringskoder/
    industry_codes = {
        k: v
        for k, v in row.items()
        if k.startswith("industry_code") and k.endswith("code")
    }
    for k, v in industry_codes.items():
        entity.add("sector", v)
        row.pop(k)

    h.apply_date(entity, "incorporationDate", get_oldest_date(row, INC_DATES))
    h.apply_date(entity, "dissolutionDate", get_oldest_date(row, DISSOLV_DATES))

    # Addresses
    for prefix in ["business_address", "postal_address"]:
        country = row.pop(f"{prefix}_country_code")
        if country and country.lower() not in entity.countries:
            entity.add("country", country.lower())
        address = h.make_address(
            context,
            street=row.pop(f"{prefix}_street"),
            city=row.pop(f"{prefix}_city"),
            postal_code=row.pop(f"{prefix}_postal_code"),
            place=row.pop(f"{prefix}_municipality"),
            country_code=country.lower() if country else None,
        )
        h.copy_address(entity, address)

    context.audit_data(row, IGNORE)
    return entity


def crawl_soe_peps(
    context: Context,
    org_number: str,
    company_name: str,
    company_juris: str,
    entity: Entity,
) -> int:
    api_org_url = f"{BASE_API}/enheter/{org_number}/roller"
    rollegrupper = context.fetch_json(api_org_url)["rollegrupper"]

    pos_in_company = 0
    for group in rollegrupper:
        group_type = group.get("type")
        group_code = group_type.get("kode")

        if group_code not in GROUP_CODES:
            for role in group.get("roller"):
                role_data = role.get("type")
                # assert that we don't omit any target roles
                assert role_data.get("kode") not in GROUP_CODES
            continue

        for role in group.get("roller"):
            role_data = role.get("type")
            role_name = role_data.get("beskrivelse")

            person_data = role.get("person")
            if person_data is None:
                continue
            names = person_data.get("navn")
            first_name = names.pop("fornavn")
            last_name = names.pop("etternavn")
            birth_date = person_data.pop("fodselsdato")

            pep = context.make("Person")
            if birth_date:
                pep.id = context.make_id(first_name, last_name, birth_date)
            else:
                pep.id = context.make_id(first_name, last_name, company_name)

            h.apply_name(pep, first_name=first_name, last_name=last_name)
            pep.add("birthDate", birth_date)
            pep.add("country", "no")

            position = h.make_position(
                context,
                name=f"{role_name}, {company_name}",
                topics=["gov.soe"],
                # adding company's country of jurisdiction, too
                country=["no", company_juris],
                organization=entity,
            )
            categorisation = categorise(context, position, is_pep=True)

            if not categorisation.is_pep:
                continue

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

            pos_in_company += 1

    assert 1 <= pos_in_company <= 40, (
        f"{company_name} (org_number: {org_number}) emitted {pos_in_company} positions; "
        "expected not more than 40 positions for board and c-level executives"
    )
    return pos_in_company


def crawl(context: Context) -> None:
    total_positions = 0
    n_companies = 0

    page = context.fetch_html(
        context.data_url, method="GET", absolute_links=True, cache_days=3
    )
    url = h.xpath_string(
        page, "//a[contains(@href, '/enhetsregisteret/api/enheter/lastned/csv')]/@href"
    )
    path = context.fetch_resource("source.csv.gz", url)
    context.export_resource(path, GZIP, title=context.SOURCE_TITLE)

    # read the norwegian header
    with gzip.open(path, "rt") as f:
        nok_header_row = next(csv.reader(f))
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
            institutional_sector = row.pop("institutional_sector_description")
            if not institutional_sector or "Statlig eide" not in institutional_sector:
                continue

            company_name = row.pop("name")
            org_number = row.pop("org_number")
            company_juris = row.pop("subject_to_legislation_country_code")
            n_companies += 1

            entity = crawl_company(
                context,
                row,
                company_name,
                org_number,
                company_juris,
            )
            total_positions += crawl_soe_peps(
                context,
                org_number,
                company_name,
                company_juris,
                entity,
            )

        assert n_companies <= total_positions, (
            f"Sanity check failed: extracted {total_positions} positions from {n_companies} SOEs, "
            "expected at least one position per company"
        )
