import gzip
import csv
from typing import Any, List, Dict

from zavod import Context, helpers as h
from rigour.mime.types import GZIP


# incorporation & dissolution dates to check
INC_DATES = [
    "registration_date_entity_register",
    "foundation_date",
    "registration_date_business_register",
]
DISSOLV_DATES = [
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
    name = dict_row.pop("name")  # what about foreign_register_name?
    org_number = dict_row.pop("org_number")

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, org_number)
    entity.add("name", name)
    entity.add(
        "jurisdiction",
        ["no", dict_row.pop("subject_to_legislation_country_code", None)],
    )
    entity.add("legalForm", dict_row.pop("org_form_description", None))
    # entity.add("description", dict_row.pop("statutory_purpose", None))
    # entity.add("notes", dict_row.pop("activity", None))

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

    entity.add("email", dict_row.pop("email"))
    website = dict_row.pop("website", None)
    if website and (
        website.isdigit()
        or any(word in website.lower() for word in ("instagram", "facebook", "google"))
    ):
        website = None
    entity.add("website", website)
    entity.add("phone", dict_row.pop("phone", None))
    entity.add("phone", dict_row.pop("phone_mobile", None))
    h.apply_date(entity, "incorporationDate", get_oldest_date(dict_row, INC_DATES))
    h.apply_date(entity, "dissolutionDate", get_oldest_date(dict_row, DISSOLV_DATES))

    country_code_business = (
        dict_row.pop("business_address_country_code", None) or ""
    ).lower() or None
    # country_code_business = context.lookup_value("country_code", country_code_business)
    # business_country = dict_row.get("business_address_country", None)
    # j = 0
    # if business_country == "Kongo" or (business_country and "kongo" in business_country.lower()):
    #     j += 1
    #     breakpoint()
    # if j > 1:
    #     breakpoint()
    business_address = h.make_address(
        context,
        street=dict_row.pop("business_address_street", None),
        city=dict_row.pop("business_address_city", None),
        postal_code=dict_row.pop("business_address_postal_code", None),
        country=dict_row.pop("business_address_country", None),
        country_code=country_code_business,
    )
    h.copy_address(entity, business_address)

    country_code_postal = (
        dict_row.pop("postal_address_country_code", None) or ""
    ).lower() or None
    # country_code_postal = context.lookup_value("country_code", country_code_postal)
    postal_address = h.make_address(
        context,
        street=dict_row.pop("postal_address_street", None),
        city=dict_row.pop("postal_address_city", None),
        postal_code=dict_row.pop("postal_address_postal_code", None),
        country=dict_row.pop("postal_address_country", None),
        country_code=country_code_postal,
    )
    h.copy_address(entity, postal_address)
    foreign_address = h.make_address(
        context,
        street=dict_row.pop("foreign_register_address_street", None),
        city=dict_row.pop("foreign_register_address_city", None),
        country=dict_row.pop("foreign_register_address_country", None),
    )
    h.copy_address(entity, foreign_address)

    context.emit(entity)

    COLS_NOT_IN_USE_SUBSTR = (
        "employee_count",
        "municipality",
        "vat_register",
        "org_register",
        "business_register",
        "foundation_register",
        "party_register",
        "opt_out",
        "capital",
        "subject_to_legislation",
        "company_form_home",
    )
    COLS_NOT_IN_USE = (
        [
            "org_form_code",
            "support_unit_code",
            "support_unit_code_description",
            "institutional_sector_code",
            "institutional_sector_description",
            "last_submitted_annual_accounts",
            "bankruptcy_date",
            "under_liquidation",
            "language_form",
            "articles_of_association_date",
            "statutory_purpose",
            "activity",
            "endorsements",
            "under_foreign_insolvency_proceedings_date",
            "under_reconstruction_negotiations_date",
            "is_in_corporate_group",
            "foreign_register_name",  # maybe we need it? as an alias maybe? it is tied to foreign address
            "parent_entity",  # do we need to add it?
        ]
        + [
            k
            for k in dict_row
            if k.startswith("industry_code") and k.endswith("description")
        ]
        + [k for k in dict_row if any(substr in k for substr in COLS_NOT_IN_USE_SUBSTR)]
    )
    context.audit_data(dict_row, ignore=COLS_NOT_IN_USE)


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
            crawl_row(context, row)
