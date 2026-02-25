from enum import Enum
import re
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any, Set, cast
from urllib.parse import urlencode

from followthemoney.types import registry
from normality import squash_spaces
from requests.exceptions import RetryError
from zavod.context import Context
from zavod.entity import Entity
from zavod.shed.trans import (
    apply_translit_full_name,
    apply_translit_names,
    make_position_translation_prompt,
    ENGLISH,
    RUSSIAN,
    ARABIC,
)
from zavod.stateful.positions import OccupancyStatus, categorise

from zavod import helpers as h

DECLARATION_LIST_URL = "https://declaration.acb.gov.ge/Home/DeclarationList"
REGEX_CHANGE_PAGE = re.compile(r"changePage\((\d+), \d+\)")
REGEX_FORMER = re.compile(r"\(ყოფილი\)", re.IGNORECASE)
_18_YEARS_AGO = (datetime.now() - timedelta(days=18 * 365)).isoformat()
TRANSLIT_OUTPUT = [ENGLISH, RUSSIAN, ARABIC]
POSITION_PROMPT = prompt = make_position_translation_prompt("kat")


class FamilyMemberStatus(Enum):
    DEFAULT = 0
    MINOR = 1


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


def crawl_enterprise(
    context: Context, *, pep: Entity, item: dict[str, Any], source_url: str
) -> None:
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
    company.add("sourceUrl", source_url)
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
        partner.add("sourceUrl", source_url)
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
    context: Context,
    *,
    minors: Set[tuple[str, str]],
    item: dict[str, Any],
    key: str,
    pep: Entity,
    source_url: str,
) -> None:
    """Crawl assets for a family member, skipping minors."""
    rels = item.pop(key)
    for rel in rels:
        first_name = rel.pop("OwnerFirstName")
        last_name = rel.pop("OwnerLatsName")

        # Skip minors
        if (first_name.casefold(), last_name.casefold()) in [
            (m[0].casefold(), m[1].casefold()) for m in minors
        ]:
            context.log.debug("Skipping minor with asset", source=source_url)
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
            context,
            person,
            input_code="kat",
            first_name=first_name,
            last_name=last_name,
            output_spec=TRANSLIT_OUTPUT,
        )
        person.add("topics", "role.rca")

        rel_entity = context.make("Family")
        rel_entity.id = context.make_id(pep.id, relationship, person.id)
        rel_entity.add("person", pep)
        rel_entity.add("relative", person)
        rel_entity.add("relationship", relationship, lang="kat")
        rel_entity.add("sourceUrl", source_url)

        context.emit(person)
        context.emit(rel_entity)


def crawl_family_member(
    context: Context, *, pep: Entity, relative: dict[str, Any], declaration_url: str
) -> tuple[str, str, FamilyMemberStatus]:
    """Crawl a family member, skip if they're a minor.

    Returns a tuple of first name, last name, and FamilyMemberStatus to determine if they're a minor."""
    first_name = relative.pop("FirstName")
    last_name = relative.pop("LastName")
    birth_date = h.extract_date(context.dataset, relative.pop("BirthDate"))

    if birth_date[0] > _18_YEARS_AGO:
        context.log.debug("Skipping minor", birth_date=birth_date)
        return first_name, last_name, FamilyMemberStatus.MINOR

    birth_place = relative.pop("BirthPlace")
    person = context.make("Person")
    # TODO(Leon Handreke): birth_date is a list, not a string,
    # but fixing thigs would mean a re-key, so avoid for now.
    person.id = context.make_id(first_name, last_name, birth_date, birth_place)  # type: ignore[arg-type]
    h.apply_name(person, first_name=first_name, last_name=last_name, lang="kat")
    apply_translit_names(
        context,
        person,
        input_code="kat",
        first_name=first_name,
        last_name=last_name,
        output_spec=TRANSLIT_OUTPUT,
    )
    person.add("birthDate", birth_date)
    person.add("birthPlace", birth_place, lang="kat")
    person.add("topics", "role.rca")

    relationship = relative.pop("Relationship")
    rel_entity = context.make("Family")
    rel_entity.id = context.make_id(pep.id, relationship, person.id)
    rel_entity.add("person", pep)
    rel_entity.add("relative", person)
    rel_entity.add("relationship", relationship, lang="kat")
    rel_entity.add("description", relative.pop("Comment"), lang="kat")
    rel_entity.add("sourceUrl", declaration_url)

    context.emit(person)
    context.emit(rel_entity)
    context.audit_data(relative)

    return first_name, last_name, FamilyMemberStatus.DEFAULT


def crawl_declaration(
    context: Context, *, item: dict[str, Any], is_current_year: bool
) -> None:
    declaration_id = item.pop("Id")
    first_name = item.pop("FirstName")
    last_name = item.pop("LastName")
    birth_place = item.pop("BirthPlace")
    birth_date = h.extract_date(context.dataset, item.pop("BirthDate"))[0]
    person = context.make("Person")
    person.id = context.make_id(first_name, last_name, birth_date, birth_place)
    h.apply_name(person, first_name=first_name, last_name=last_name, lang="kat")
    apply_translit_names(
        context,
        person,
        input_code="kat",
        first_name=first_name,
        last_name=last_name,
        output_spec=TRANSLIT_OUTPUT,
    )
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

    minor_family_member_names: Set[tuple[str, str]] = set()
    for rel in item.pop("FamilyMembers"):
        first_name, last_name, status = crawl_family_member(
            context, pep=person, relative=rel, declaration_url=declaration_url
        )
        if status == FamilyMemberStatus.MINOR:
            minor_family_member_names.add((first_name, last_name))

    crawl_assets_for_family(
        context,
        minors=minor_family_member_names,
        item=item,
        key="Properties",
        pep=person,
        source_url=declaration_url,
    )
    crawl_assets_for_family(
        context,
        minors=minor_family_member_names,
        item=item,
        key="MovableProperties",
        pep=person,
        source_url=declaration_url,
    )
    crawl_assets_for_family(
        context,
        minors=minor_family_member_names,
        item=item,
        key="Securities",
        pep=person,
        source_url=declaration_url,
    )
    crawl_assets_for_family(
        context,
        minors=minor_family_member_names,
        item=item,
        key="BankAccounts",
        pep=person,
        source_url=declaration_url,
    )
    crawl_assets_for_family(
        context,
        minors=minor_family_member_names,
        item=item,
        key="Cashes",
        pep=person,
        source_url=declaration_url,
    )
    for enterprise in item.get("Enterprice", []):
        crawl_enterprise(
            context, pep=person, item=deepcopy(enterprise), source_url=declaration_url
        )
    crawl_assets_for_family(
        context,
        minors=minor_family_member_names,
        item=item,
        key="Enterprice",
        pep=person,
        source_url=declaration_url,
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


def fetch_declarations(
    context: Context, *, year: int, name: str, cache_days: int
) -> list[dict[str, Any]]:
    query = {
        "Key": name,
        "YearSelectedValues": year,
    }
    url = f"{context.data_url}?{urlencode(query)}"
    try:
        declarations = context.fetch_json(url, cache_days=cache_days)
        return cast(list[dict[str, Any]], declarations)
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
            page_count_el = h.xpath_element(
                doc, ".//li[@class='PagedList-skipToLast']/a"
            )
            page_count_match = REGEX_CHANGE_PAGE.match(page_count_el.get("onclick", ""))
            max_page = int(page_count_match.group(1)) if page_count_match else None

            cache_days = 7 if year == current_year else 30

            for row in doc.findall(".//div[@class='declaration1']"):
                name = h.element_text(h.xpath_elements(row, ".//h3")[0])
                declarations = fetch_declarations(
                    context, year=year, name=name, cache_days=cache_days
                )
                for declaration in declarations:
                    crawl_declaration(
                        context,
                        item=declaration,
                        is_current_year=(year == current_year),
                    )

            page += 1
