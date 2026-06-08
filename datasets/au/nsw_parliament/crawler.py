from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

BASE_URL = "https://www.parliament.nsw.gov.au"

POSITIONS: dict[str, dict[str, str]] = {
    "LA": {
        "name": "Member of the New South Wales Legislative Assembly",
        "wikidata_id": "Q19202748",
    },
    "LC": {
        "name": "Member of the New South Wales Legislative Council",
        "wikidata_id": "Q18810377",
    },
}


def crawl_member(
    context: Context,
    house_positions: dict[str, tuple[Entity, PositionCategorisation]],
    row: Element,
) -> None:
    cells = h.xpath_elements(row, "./td")
    if len(cells) < 8:
        return

    # Name cell contains an anchor with "Surname, FirstName" text
    raw_name = h.element_text(cells[0])
    if "," not in raw_name:
        context.log.warning("Unexpected name format", name=raw_name)
        return
    last_name, first_name = raw_name.split(",", 1)
    last_name = last_name.strip()
    first_name = first_name.strip()

    # Profile link pk is used as the stable entity ID
    hrefs = h.xpath_strings(cells[0], ".//a/@href")
    if not hrefs:
        context.log.warning("No profile link in name cell", name=raw_name)
        return
    href = hrefs[0]
    if "pk=" not in href:
        context.log.warning("Unexpected profile link format", href=href)
        return
    pk = href.split("pk=", 1)[1]
    profile_url = href if href.startswith("http") else BASE_URL + href

    house = h.element_text(cells[4])
    party = h.element_text(cells[6]) or None
    gender = h.element_text(cells[7]) or None

    if house not in house_positions:
        context.log.warning("Unknown house code", house=house)
        return

    position, categorisation = house_positions[house]

    # Extract electorate from Position column (LA members only; LC members are
    # elected statewide and have no single electorate).
    constituency: str | None = None
    for li in h.xpath_elements(cells[1], ".//li"):
        li_text = h.element_text(li)
        if li_text.startswith("Member for "):
            constituency = li_text.removeprefix("Member for ").strip()
            break

    person = context.make("Person")
    person.id = context.make_slug("member", pk)
    h.apply_name(person, first_name=first_name, last_name=last_name, lang="eng")
    person.add("political", party)
    person.add("gender", gender)
    person.add("sourceUrl", profile_url)
    # Candidates must be enrolled to vote; enrolment requires Australian
    # citizenship: Electoral Act 2017 (NSW), ss 30 and 83.
    # https://legislation.nsw.gov.au/view/html/inforce/current/act-2017-066
    person.add("citizenship", "au")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        no_end_implies_current=True,
        propagate_country=True,
    )
    if occupancy is not None:
        if constituency is not None:
            occupancy.add("constituency", constituency)
        context.emit(occupancy)
        context.emit(person)


def crawl(context: Context) -> None:
    house_positions: dict[str, tuple[Entity, PositionCategorisation]] = {}
    for house_code, config in POSITIONS.items():
        position = h.make_position(
            context,
            name=config["name"],
            country="au",
            wikidata_id=config["wikidata_id"],
        )
        categorisation = categorise(context, position, default_is_pep=True)
        context.emit(position)
        house_positions[house_code] = (position, categorisation)

    doc = context.fetch_html(context.data_url, absolute_links=True)
    table = h.xpath_element(doc, "//table[@id='prlMembers']")
    # Header is in <thead>; direct ./tr children are all data rows.
    for row in h.xpath_elements(table, "./tr"):
        crawl_member(context, house_positions, row)
