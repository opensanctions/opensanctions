import re
from copy import deepcopy
from datetime import datetime, timedelta
from typing import List, Set
from urllib.parse import urlencode

from followthemoney.types import registry
from normality import slugify, squash_spaces
from requests.exceptions import RetryError
from zavod.context import Context
from zavod.entity import Entity
from zavod.shed.trans import (
    apply_translit_full_name,
    apply_translit_names,
    make_position_translation_prompt,
)
from zavod.stateful.positions import OccupancyStatus, categorise

from zavod import helpers as h

DECLARATION_LIST_URL = "https://declaration.acb.gov.ge/Home/DeclarationList"
REGEX_CHANGE_PAGE = re.compile(r"changePage\((\d+), \d+\)")
REGEX_FORMER = re.compile(r"\(ყოფილი\)", re.IGNORECASE)
_18_YEARS_AGO = (datetime.now() - timedelta(days=18 * 365)).isoformat()
TRANSLIT_OUTPUT = {
    "eng": ("Latin", "English"),
    "rus": ("Cyrillic", "Russian"),
    "ara": ("Arabic", "Arabic"),
}
POSITION_PROMPT = prompt = make_position_translation_prompt("kat")


def name_slug(first_name, last_name):
    return slugify(first_name, last_name)


# This seems a bit too distant to put time into verifying properly right now.
# e.g. https://declaration.acb.gov.ge/Home/DownloadPdf/155243
# The PEP is Koba Kobaladze
# Their family member is Ketevan Jabakhidze
# Delis LLC is linked to Ketevan
# https://www.companyinfo.ge/en/corporations/448641
# The link isn't directly to the company, but apparently to a related person
#
# def crawl_linked_enterprise(
#     context: Context, pep: Entity, item: dict, source: str
# ) -> None:
#     print(item)
#     company = context.make("Company")
#     name = item.pop("Name")
#     company.id = context.make_id(name)
#     company.add("name", name, lang="kat")
#     apply_translit_full_name(context, company, "kat", name, TRANSLIT_OUTPUT)
#     company.add("sourceUrl", source)
#     context.emit(company)
#
#     pep_link = context.make("UnknownLink")
#     pep_link.id = context.make_id(pep.id, "linked enterprise", company.id)
#     !!! see note above pep_link.add("subject", pep)
#     pep_link.add("object", company)
#     context.emit(pep_link)
#
#     enterprice_name = item.pop("EnterpriceName")  # sic
#     linked_company = context.make("Company")
#     linked_company.id = context.make_id(enterprice_name)
#     linked_company.add("name", enterprice_name, lang="kat")
#     apply_translit_full_name(
#         context, linked_company, "kat", enterprice_name, TRANSLIT_OUTPUT
#     )
#     linked_company.add("sourceUrl", source)
#     context.emit(linked_company)
#
#     company_link = context.make("Ownership")
#     company_link.id = context.make_id(pep.id, "owns", linked_company.id)
#     company_link.add("owner", company)
#     company_link.add("asset", linked_company)
#     company_link.add("percentage", item.pop("Share"))
#     context.emit(company_link)
#
#     for partner_dict in item.pop("EnterprisePartners"):
#         partner_name = partner_dict.pop("FullName")
#         if partner_name is None:
#             continue
#         partner = context.make("LegalEntity")
#         partner.id = context.make_id(partner_name)
#         partner.add("name", partner_name, lang="kat")
#         apply_translit_full_name(context, partner, "kat", partner_name, TRANSLIT_OUTPUT)
#         partner.add("address", partner_dict.pop("LegalAddress"), lang="kat")
#         partner.add("sourceUrl", source)
#         context.emit(partner)
#
#         rel = context.make("UnknownLink")
#         rel.id = context.make_id(pep.id, "partner of linked enterprise", partner.id)
#         rel.add("subject", linked_company)
#         rel.add("object", partner)
#         context.emit(rel)


def crawl_enterprise(context: Context, pep: Entity, item: dict, source: str) -> None:
    first_name = item.pop("OwnerFirstName")
    last_name = item.pop("OwnerLatsName")
    name = item.pop("Name")
    # Skip family member enterprises
    if not (first_name in pep.get("firstName") and last_name in pep.get("lastName")):
        return
    if item.pop("RelationName"):
        return
    legal_form = item.pop("PartnershipFormName")
    if legal_form == "ინდივიდუალური მეწარმე":  # Individual enterprise
        return
    company = context.make("Company")
    company.id = context.make_id(name)
    company.add("name", name, lang="kat")
    apply_translit_full_name(
        context, entity=company, input_code="kat", name=name, output=TRANSLIT_OUTPUT
    )
    company.add("address", item.pop("EnterpriseAddress"), lang="kat")
    h.apply_date(company, "incorporationDate", item.pop("RegisterDate"))
    company.add("legalForm", legal_form, lang="kat")
    company.add("sourceUrl", source)
    context.emit(company)

    ownership = context.make("Ownership")
    ownership.id = context.make_id(pep.id, "owns", company.id)
    ownership.add("owner", pep)
    ownership.add("asset", company)
    ownership.add("percentage", item.pop("Share"))
    h.apply_date(ownership, "startDate", item.pop("StartDate"))
    h.apply_date(ownership, "endDate", item.pop("EndDate", None))
    ownership.add("description", item.pop("Comment", None), lang="kat")
    context.emit(ownership)

    for partner_dict in item.pop("EnterprisePartners"):
        partner_name = partner_dict.pop("FullName")
        if partner_name is None:
            continue
        address = partner_dict.pop("LegalAddress")
        partner = context.make("LegalEntity")
        partner.id = context.make_id(company.id, partner_name, address)
        partner.add("name", partner_name, lang="kat")
        apply_translit_full_name(
            context,
            entity=partner,
            input_code="kat",
            name=partner_name,
            output=TRANSLIT_OUTPUT,
        )
        partner.add("address", address, lang="kat")
        partner.add("sourceUrl", source)
        context.emit(partner)

        rel = context.make("UnknownLink")
        rel.id = context.make_id(pep.id, "partner of linked enterprise", partner.id)
        rel.add("subject", company)
        rel.add("object", partner)
        context.emit(rel)

    if linked := item.pop("LinkedEnterprises"):
        context.log.warning("Linked enterprises not handled", linked=linked)
    context.audit_data(
        item,
        ignore=[
            "OwnerId",
            "EndDateType",
            "EndDateName",
            "RegistrationAgency",
            "Income",
            "IncomeCurrencyName",
            "AbbreviationName",
            "OtherAbbreviationName",
            "AbbreviationTypeId",
        ],
    )


def crawl_assets_for_family(
    context: Context, minors: Set[str], item: dict, key: str, pep: Entity, source: str
):
    rels = item.pop(key)
    for rel in rels:
        first_name = rel.pop("OwnerFirstName")
        last_name = rel.pop("OwnerLatsName")
        slug = name_slug(first_name, last_name)
        if slug in minors:
            context.log.debug("Skipping minor with asset", source=source)
            return
        relationship = rel.pop("RelationName")
        # skip themselves, just care about family
        if first_name in pep.get("firstName") and last_name in pep.get("lastName"):
            continue
        if not relationship:
            continue
        person = context.make("Person")
        person.id = context.make_id(first_name, last_name, relationship, pep.id)
        h.apply_name(person, first_name=first_name, last_name=last_name, lang="kat")
        apply_translit_names(
            context, person, "kat", first_name, last_name, TRANSLIT_OUTPUT
        )
        person.add("topics", "role.rca")

        rel_entity = context.make("Family")
        rel_entity.id = context.make_id(pep.id, relationship, person.id)
        rel_entity.add("person", pep)
        rel_entity.add("relative", person)
        rel_entity.add("relationship", relationship, lang="kat")
        rel_entity.add("sourceUrl", source)

        context.emit(person)
        context.emit(rel_entity)


def crawl_family(
    context: Context, pep: Entity, rel: dict, declaration_url: str
) -> str | None:
    """Returns slug of family member if they're a minor"""
    first_name = rel.pop("FirstName")
    last_name = rel.pop("LastName")
    birth_date = h.extract_date(context.dataset, rel.pop("BirthDate"))
    if birth_date[0] > _18_YEARS_AGO:
        context.log.debug("Skipping minor", birth_date=birth_date)
        return name_slug(first_name, last_name)
    birth_place = rel.pop("BirthPlace")
    person = context.make("Person")
    person.id = context.make_id(first_name, last_name, birth_date, birth_place)
    h.apply_name(person, first_name=first_name, last_name=last_name, lang="kat")
    apply_translit_names(context, person, "kat", first_name, last_name, TRANSLIT_OUTPUT)
    person.add("birthDate", birth_date)
    person.add("birthPlace", birth_place, lang="kat")
    person.add("topics", "role.rca")

    relationship = rel.pop("Relationship")
    rel_entity = context.make("Family")
    rel_entity.id = context.make_id(pep.id, relationship, person.id)
    rel_entity.add("person", pep)
    rel_entity.add("relative", person)
    rel_entity.add("relationship", relationship, lang="kat")
    rel_entity.add("description", rel.pop("Comment"), lang="kat")
    rel_entity.add("sourceUrl", declaration_url)

    context.emit(person)
    context.emit(rel_entity)
    context.audit_data(rel)


def crawl_declaration(context: Context, item: dict, is_current_year) -> None:
    declaration_id = item.pop("Id")
    first_name = item.pop("FirstName")
    last_name = item.pop("LastName")
    birth_place = item.pop("BirthPlace")
    birth_date = h.extract_date(context.dataset, item.pop("BirthDate"))[0]
    person = context.make("Person")
    person.id = context.make_id(first_name, last_name, birth_date, birth_place)
    h.apply_name(person, first_name=first_name, last_name=last_name, lang="kat")
    apply_translit_names(context, person, "kat", first_name, last_name, TRANSLIT_OUTPUT)
    person.add("birthDate", birth_date)
    person.add("birthPlace", birth_place, lang="kat")
    declaration_url = (
        f"https://declaration.acb.gov.ge/Home/DownloadPdf/{declaration_id}"
    )
    person.add("sourceUrl", declaration_url)

    position_name_kat = item.pop("Position")
    occupancy_description = None
    organization = item.pop("Organisation")

    if "კანდიდატი" in position_name_kat:  # Candidate
        context.log.debug(f"Skipping candidate {position_name_kat}")
        return

    position_name_kat = squash_spaces(REGEX_FORMER.sub("", position_name_kat))

    if len(position_name_kat) < 35:
        position_name_kat = f"{position_name_kat}, {organization}"

    if len(position_name_kat) > registry.name.max_length:
        occupancy_description = position_name_kat
        position_name_kat = (
            context.lookup_value("positions", position_name_kat) or position_name_kat
        )

    position = h.make_position(
        context,
        position_name_kat,
        country="ge",
        lang="kat",
    )
    apply_translit_full_name(
        context, position, "kat", position_name_kat, TRANSLIT_OUTPUT, POSITION_PROMPT
    )
    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        return
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        status=OccupancyStatus.CURRENT if is_current_year else OccupancyStatus.UNKNOWN,
    )
    if not occupancy:
        return
    occupancy.add("description", occupancy_description, lang="kat")
    context.emit(position)
    context.emit(occupancy)
    context.emit(person)

    minor_family = set()
    for rel in item.pop("FamilyMembers"):
        minor = crawl_family(context, person, rel, declaration_url)
        if minor:
            minor_family.add(minor)

    crawl_assets_for_family(
        context, minor_family, item, "Properties", person, declaration_url
    )
    crawl_assets_for_family(
        context, minor_family, item, "MovableProperties", person, declaration_url
    )
    crawl_assets_for_family(
        context, minor_family, item, "Securities", person, declaration_url
    )
    crawl_assets_for_family(
        context, minor_family, item, "BankAccounts", person, declaration_url
    )
    crawl_assets_for_family(
        context, minor_family, item, "Cashes", person, declaration_url
    )
    for enterprise in item.get("Enterprice"):
        crawl_enterprise(context, person, deepcopy(enterprise), declaration_url)
    crawl_assets_for_family(
        context, minor_family, item, "Enterprice", person, declaration_url
    )

    # for enterprise in item.pop("LinkedEnterprice"):
    #    crawl_linked_enterprise(context, person, enterprise, declaration_url)

    context.audit_data(
        item,
        ignore=[
            "LinkedEnterprice",
            "Organisation",
            "VersionId",
            "DateEdited",
            "DeclarationSubmitDate",
            "Jobs",
            "Contracts",
            "Gifts",
            "InOuts",
            "DeclarationHistory",
        ],
    )


def query_declaration(
    context: Context, year: int, name: str, cache_days: int
) -> List[dict]:
    query = {
        "Key": name,
        "YearSelectedValues": year,
    }
    url = f"{context.data_url}?{urlencode(query)}"
    try:
        declarations = context.fetch_json(url, cache_days=cache_days)
        return declarations
    except RetryError as e:
        if context.lookup("known_errors", name):
            context.log.info(
                f"Known error querying declaration: {e}", name=name, year=year, url=url
            )
            return []
        else:
            context.log.warning(
                f"Error querying declaration: {e}", name=name, year=year, url=url
            )
            return []


def crawl(context: Context) -> None:
    current_year = datetime.now().year
    for year in [current_year, current_year - 1]:
        page = 1
        max_page = None

        while max_page is None or page <= max_page:
            url = f"{DECLARATION_LIST_URL}?yearSelectedValues={year}&page={page}"
            cache_days = 1 if year == current_year else 30
            doc = context.fetch_html(url, cache_days=cache_days)
            page_count_el = doc.find(".//li[@class='PagedList-skipToLast']/a")
            page_count_match = REGEX_CHANGE_PAGE.match(page_count_el.get("onclick"))
            max_page = int(page_count_match.group(1))
            cache_days = 7 if year == current_year else 30

            for row in doc.findall(".//div[@class='declaration1']"):
                name = row.find(".//h3").text_content()
                declarations = query_declaration(context, year, name, cache_days)
                for declaration in declarations:
                    crawl_declaration(context, declaration, year == current_year)

            page += 1
