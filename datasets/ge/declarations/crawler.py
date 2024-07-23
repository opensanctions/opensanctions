from urllib.parse import urlencode
from datetime import datetime, timedelta
import re

from zavod.context import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.logic.pep import categorise, OccupancyStatus


DECLARATION_LIST_URL = "https://declaration.acb.gov.ge/Home/DeclarationList"
REGEX_CHANGE_PAGE = re.compile(r"changePage\((\d+), \d+\)")
FORMATS = ["%d.%m.%Y"]  # 04.12.2023
_18_YEARS_AGO = (datetime.now() - timedelta(days=18 * 365)).isoformat()


def crawl_family(
    context: Context, pep: Entity, rel: dict, declaration_url: str
) -> None:
    first_name = rel.pop("FirstName")
    last_name = rel.pop("LastName")
    birth_date = h.parse_date(rel.pop("BirthDate"), FORMATS)
    if birth_date[0] > _18_YEARS_AGO:
        context.log.debug("Skipping minor", birth_date=birth_date)
        return
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
        print(f"Skipping candidate {position}")
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

    for rel in item.pop("FamilyMembers"):
        crawl_family(context, person, rel, declaration_url)


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
