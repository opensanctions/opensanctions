from urllib.parse import urlencode
from datetime import datetime
import re

from zavod.context import Context
from zavod import helpers as h
from zavod.logic.pep import categorise, OccupancyStatus


DECLARATION_LIST_URL = "https://declaration.acb.gov.ge/Home/DeclarationList"
REGEX_CHANGE_PAGE = re.compile(r"changePage\((\d+), \d+\)")
FORMATS = ["%d.%m.%Y"]  # 04.12.2023


def crawl_declaration(context: Context, item: dict, is_current_year) -> None:
    first_name = item.pop("FirstName")
    last_name = item.pop("LastName")
    birth_place = item.pop("BirthPlace")
    birth_date = h.parse_date(item.pop("BirthDate"), FORMATS)
    person = context.make("Person")
    person.id = context.make_id(first_name, last_name, birth_date, birth_place)
    h.apply_name(person, first_name=first_name, last_name=last_name)
    person.add("birthDate", birth_date)
    person.add("birthPlace", birth_place)

    position = item.pop("Position")
    if "კანდიდატი" in position:  # Candidate
        print(f"Skipping candidate {position}")
        return

    position = h.make_position(
        context,
        position,
        country="ge",
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

    family_members = item.pop("FamilyMembers")


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

        while max_page == None or page <= max_page:
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
