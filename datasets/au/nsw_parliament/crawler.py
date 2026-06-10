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
        # The chamber word as it appears in the detail-page position tables.
        "chamber": "Assembly",
    },
    "LC": {
        "name": "Member of the New South Wales Legislative Council",
        "wikidata_id": "Q18810377",
        "chamber": "Council",
    },
}


def extract_term_start(detail: Element, chamber: str) -> str | None:
    """Return the date the member's current term in the given chamber began.

    The start date lives in the member's current-positions table on their
    profile page. That table's accent colour differs between the two chambers
    (Assembly vs. Council), so we match on its header columns rather than the
    CSS class.
    """
    target = f"Member of the NSW Legislative {chamber}"
    for table in h.xpath_elements(detail, "//table"):
        rows = h.xpath_elements(table, ".//tr")
        if not rows:
            continue
        header = [h.element_text(c) for c in h.xpath_elements(rows[0], "./th | ./td")]
        if header[:3] != ["Position", "Start", "Notes"]:
            continue
        for row in rows[1:]:
            cells = h.xpath_elements(row, "./td")
            if len(cells) < 2:
                continue
            if h.element_text(cells[0]) == target:
                return h.element_text(cells[1]) or None
    return None


def extract_biography(detail: Element) -> str | None:
    """Return the member's biography, one titled section per paragraph.

    The biography is split into sections (party activity, community activity,
    etc.); empty or hidden sections are skipped.
    """
    sections: list[str] = []
    xpath = "//div[contains(@class, 'biography')]//div[contains(@class, 'biopar')]"
    for par in h.xpath_elements(detail, xpath):
        # Items within a paragraph are separated by <br>, which text_content()
        # would otherwise run together ("...present.1992..."); insert spacing.
        for br in h.xpath_elements(par, ".//br"):
            br.tail = " " + (br.tail or "")
        titles = h.xpath_elements(par, ".//span[contains(@class, 'spn-bio-title')]")
        title = h.element_text(titles[0]) if titles else ""
        body = " ".join(
            h.element_text(p) for p in h.xpath_elements(par, ".//p")
        ).strip()
        if not body:
            continue
        sections.append(f"{title}: {body}" if title else body)
    if not sections:
        return None
    return "\n\n".join(sections)


def crawl_member(
    context: Context,
    house_positions: dict[str, tuple[Entity, PositionCategorisation, str]],
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

    position, categorisation, chamber = house_positions[house]

    # Extract electorate from Position column (LA members only; LC members are
    # elected statewide and have no single electorate).
    constituency: str | None = None
    for li in h.xpath_elements(cells[1], ".//li"):
        li_text = h.element_text(li)
        if li_text.startswith("Member for "):
            constituency = li_text.removeprefix("Member for ").strip()
            break

    # The listing has no term dates or biography; both live on the profile page.
    detail = context.fetch_html(profile_url, cache_days=14)
    start_date = extract_term_start(detail, chamber)
    biography = extract_biography(detail)

    person = context.make("Person")
    person.id = context.make_slug("member", pk)
    h.apply_name(person, first_name=first_name, last_name=last_name, lang="eng")
    person.add("political", party)
    person.add("gender", gender)
    person.add("sourceUrl", profile_url)
    person.add("biography", biography)
    # Candidates must be enrolled to vote; enrolment requires Australian
    # citizenship: Electoral Act 2017 (NSW), ss 30 and 83.
    # https://legislation.nsw.gov.au/view/html/inforce/current/act-2017-066
    person.add("citizenship", "au")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        start_date=start_date,
    )
    if occupancy is not None:
        if constituency is not None:
            occupancy.add("constituency", constituency)
        context.emit(occupancy)
        context.emit(person)


def crawl(context: Context) -> None:
    house_positions: dict[str, tuple[Entity, PositionCategorisation, str]] = {}
    for house_code, config in POSITIONS.items():
        position = h.make_position(
            context,
            name=config["name"],
            country="au",
            wikidata_id=config["wikidata_id"],
        )
        categorisation = categorise(context, position, default_is_pep=True)
        context.emit(position)
        house_positions[house_code] = (position, categorisation, config["chamber"])

    doc = context.fetch_html(context.data_url, absolute_links=True)
    table = h.xpath_element(doc, "//table[@id='prlMembers']")
    # Header is in <thead>; direct ./tr children are all data rows.
    for row in h.xpath_elements(table, "./tr"):
        crawl_member(context, house_positions, row)
