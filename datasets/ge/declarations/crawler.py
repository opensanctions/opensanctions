from pprint import pprint
from typing import List, Set
from urllib.parse import urlencode
from datetime import datetime, timedelta
import re

from normality import slugify

from zavod.context import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.logic.pep import categorise, OccupancyStatus


DECLARATION_LIST_URL = "https://declaration.acb.gov.ge/Home/DeclarationList"
REGEX_CHANGE_PAGE = re.compile(r"changePage\((\d+), \d+\)")
FORMATS = ["%d.%m.%Y"]  # 04.12.2023
_18_YEARS_AGO = (datetime.now() - timedelta(days=18 * 365)).isoformat()


def name_slug(first_name, last_name):
    return slugify(first_name, last_name)


def crawl_linked_enterprise(context: Context, pep: Entity, item: dict, source: str) -> None:
    company = context.make("Company")
    name = item.pop("Name")
    company.id = context.make_id(name)
    company.add("name", name, lang="geo")
    company.add("sourceUrl", source)
    context.emit(company)

    pep_link = context.make("UnknownLink")
    pep_link.id = context.make_id(pep.id, "linked enterprise", company.id)
    pep_link.add("subject", pep)
    pep_link.add("object", company)
    context.emit(pep_link)

    enterprice_name = item.pop("EnterpriceName") # sic
    linked_company = context.make("Company")
    linked_company.id = context.make_id(enterprice_name)
    linked_company.add("name", enterprice_name, lang="geo")
    linked_company.add("sourceUrl", source)
    context.emit(linked_company)

    company_link = context.make("Ownership")
    company_link.id = context.make_id(pep.id, "owns", linked_company.id)
    company_link.add("owner", company)
    company_link.add("asset", linked_company)
    company_link.add("percentage", item.pop("Share"))
    context.emit(company_link)

    for partner_dict in item.pop("EnterprisePartners"):
        partner_name = partner_dict.pop("FullName")
        if partner_name is None:
            continue
        partner = context.make("LegalEntity")
        partner.id = context.make_id(partner_name)
        partner.add("name", partner_name, lang="geo")
        partner.add("address", partner_dict.pop("LegalAddress"), lang="geo")
        partner.add("sourceUrl", source)
        context.emit(partner)

        rel = context.make("UnknownLink")
        rel.id = context.make_id(pep.id, "partner of linked enterprise", partner.id)
        rel.add("subject", linked_company)
        rel.add("object", partner)
        context.emit(rel)


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
        person.id = context.make_id(last_name, last_name, relationship, pep.id)
        h.apply_name(person, first_name=first_name, last_name=last_name, lang="geo")
        person.add("topics", "role.rca")

        rel_entity = context.make("Family")
        rel_entity.id = context.make_id(pep.id, relationship, person.id)
        rel_entity.add("person", pep)
        rel_entity.add("relative", person)
        rel_entity.add("relationship", relationship, lang="geo")
        rel_entity.add("sourceUrl", source)

        context.emit(person)
        context.emit(rel_entity)


def crawl_family(
    context: Context, pep: Entity, rel: dict, declaration_url: str
) -> str | None:
    """Returns slug of family member if they're a minor"""
    first_name = rel.pop("FirstName")
    last_name = rel.pop("LastName")
    birth_date = h.parse_date(rel.pop("BirthDate"), FORMATS)
    if birth_date[0] > _18_YEARS_AGO:
        context.log.debug("Skipping minor", birth_date=birth_date)
        return name_slug(first_name, last_name)
    birth_place = rel.pop("BirthPlace")
    person = context.make("Person")
    person.id = context.make_id(first_name, last_name, birth_date, birth_place)
    h.apply_name(person, first_name=first_name, last_name=last_name, lang="geo")
    person.add("birthDate", birth_date)
    person.add("birthPlace", birth_place, lang="geo")
    person.add("topics", "role.rca")

    relationship = rel.pop("Relationship")
    rel_entity = context.make("Family")
    rel_entity.id = context.make_id(pep.id, relationship, person.id)
    rel_entity.add("person", pep)
    rel_entity.add("relative", person)
    rel_entity.add("relationship", relationship, lang="geo")
    rel_entity.add("description", rel.pop("Comment"), lang="geo")
    rel_entity.add("sourceUrl", declaration_url)

    context.emit(person)
    context.emit(rel_entity)
    context.audit_data(rel)


def crawl_declaration(context: Context, item: dict, is_current_year) -> None:
    declaration_id = item.pop("Id")
    first_name = item.pop("FirstName")
    last_name = item.pop("LastName")
    birth_place = item.pop("BirthPlace")
    birth_date = h.parse_date(item.pop("BirthDate"), FORMATS)
    person = context.make("Person")
    person.id = context.make_id(first_name, last_name, birth_date, birth_place)
    h.apply_name(person, first_name=first_name, last_name=last_name, lang="geo")
    person.add("birthDate", birth_date)
    person.add("birthPlace", birth_place, lang="geo")
    declaration_url = (
        f"https://declaration.acb.gov.ge/Home/DownloadPdf/{declaration_id}"
    )
    person.add("sourceUrl", declaration_url)

    position = item.pop("Position")
    organization = item.pop("Organisation")
    if len(position) < 30:
        position = f"{position}, {organization}"
    if "კანდიდატი" in position:  # Candidate
        context.log.debug(f"Skipping candidate {position}")
        return

    position = h.make_position(
        context,
        position,
        country="ge",
        lang="geo",
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

    context.emit(position)
    context.emit(occupancy)
    context.emit(person, target=True)

    minor_family = set()
    for rel in item.pop("FamilyMembers"):
        minor = crawl_family(context, person, rel, declaration_url)
        if minor:
            minor_family.add(minor)

    crawl_assets_for_family(context, minor_family, item, "Properties", person, declaration_url)
    crawl_assets_for_family(
        context, minor_family, item, "MovableProperties", person, declaration_url
    )
    crawl_assets_for_family(context, minor_family, item, "Securities", person, declaration_url)
    crawl_assets_for_family(
        context, minor_family, item, "BankAccounts", person, declaration_url
    )
    crawl_assets_for_family(context, minor_family, item, "Cashes", person, declaration_url)
    crawl_assets_for_family(context, minor_family, item, "Enterprice", person, declaration_url)

    for enterprise in item.pop("LinkedEnterprice"):
        crawl_linked_enterprise(context, person, enterprise, declaration_url)

    context.audit_data(
        item,
        ignore=[
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


def query_declaration(context: Context, year: int, name: str, cache_days: int) -> None:
    query = {
        "Key": name,
        "YearSelectedValues": year,
    }
    url = f"{context.data_url}?{urlencode(query)}"
    declarations = context.fetch_json(url, cache_days=cache_days)
    return declarations


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
