import re
from typing import Optional

from datapatch import Lookup
from lxml.etree import _Element
from lxml.html import HtmlElement
from zavod.stateful.positions import OccupancyStatus, categorise

from zavod import Context
from zavod import helpers as h

# NOTE: Bundesrat members are regional executives (Minister-Presidents, Senators,
# etc.) who hold their Bundesrat seat by virtue of their regional office. We label
# all as "Mitglied des Bundesrates" as suggested by the website, since this is the
# federal legislative role relevant for PEP tracking. Status is set to UNKNOWN
# because members alternate between deputy and full member roles in ways not clearly
# documented on the source website.

# Geboren am DD.MM.YYYY
# Geboren YYYY
DOB_REGEX = re.compile(r"^Geboren(?: am\s+(\d{1,2}\.\d{2}\.\d{4})| (\d{4}))")
# Some members have no positons/membership details listed below their name (not the same as the biography)
NO_DETAILS_LIST = [
    "https://www.bundesrat.de/SharedDocs/personen/DE/laender/nw/feller-dorothee.html"
]


def extract_dob(context: Context, lookup: Lookup, text: str) -> Optional[str]:
    """Try to extract a date from text using regex, fallback to context lookup, log if missing."""
    match = DOB_REGEX.search(text)
    if match:
        return match.group(1)
    # Fallback to context lookup
    res = lookup.match(text)
    if res:
        return res.value
    context.log.warning(f"No {lookup} found for biography.", biography=text)
    return None


def extract_position_and_memberships(
    context: Context, details: _Element, url: str
) -> tuple[Optional[str], list[str]]:
    position_el = details.find(".//h2")
    memberships_els = details.findall(".//ul/li")
    if position_el is None and not memberships_els:
        if url not in NO_DETAILS_LIST:
            context.log.warning(f"{url}: missing position name or memberships")
        return None, []
    else:
        position_name = h.element_text(position_el)
        memberships = [h.element_text(el) for el in memberships_els]
        return position_name, memberships


def crawl_item(context: Context, item: HtmlElement) -> None:
    url = item.find(".//a").get("href")
    assert url, "No URL found for member"
    member_doc = context.fetch_html(url, cache_days=3)
    details = member_doc.find(".//div[@class='text-box']")
    assert details is not None and details.text is not None, (
        f"No details found for {url}"
    )
    if "N. N." in h.element_text(details):
        context.log.info("Skipping member with no name", url=url)
        return

    name_party = h.element_text(details.find(".//h1"))
    name, party = name_party.split(" |", 1)
    assert name and party, f"Missing name or party for {url}"
    position_name, memberships = extract_position_and_memberships(context, details, url)
    biography_el = h.xpath_elements(
        member_doc,
        ".//div[@class='module-box']/h1[text()='Zur Person']/following-sibling::div[@class='row']",
        expect_exactly=1,
    )
    biography = h.element_text(biography_el[0])
    dob = extract_dob(context, context.get_lookup("birth_dates"), biography)

    person = context.make("Person")
    person.id = context.make_id(name, party, position_name)
    person.add("name", name)
    person.add("political", party)
    person.add("position", position_name)
    person.add("description", biography)
    person.add("sourceUrl", url)
    person.add("citizenship", "de")
    h.apply_date(person, "birthDate", dob)
    for membership in memberships:
        person.add("position", membership.strip())

    position = h.make_position(
        context,
        name="Mitglied des Bundesrates",
        country="de",
        topics=["gov.legislative", "gov.national"],
        lang="deu",
        wikidata_id="Q15835370",
    )
    categorisation = categorise(context, position, True)

    if categorisation.is_pep:
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            categorisation=categorisation,
            no_end_implies_current=False,
            status=OccupancyStatus.UNKNOWN,
        )
        if occupancy:
            context.emit(position)
            context.emit(occupancy)
            context.emit(person)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=3, absolute_links=True)
    container = doc.find(".//div[@class='row']/ul[@class='members-list']")
    assert container is not None
    for item in h.xpath_elements(container, ".//li[@class='even' or @class='odd']"):
        crawl_item(context, item)
