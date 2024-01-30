"""
Crawl the website of the National Assembly of Venezuela and output
PEP entities for its members.
"""

import re
from typing import Iterator
from urllib.parse import parse_qs, urlparse

from zavod import Context, Entity
from zavod import helpers as h
from zavod.logic.pep import categorise
from zavod.util import ElementOrTree
from lxml.html import HtmlElement

BIRTHDATE = re.compile(r"fecha de nacimiento\s*:\s*(.*)$", re.I | re.MULTILINE)
BIRTHPLACE = re.compile(r"lugar de nacimiento\s*:\s*(.*)$", re.I | re.MULTILINE)
IDENTITY = re.compile(r"(?:c\.i|cédula de identidad)\s*:\s*(\S+)", re.I)
WS = re.compile(r"\s+")


def extract_marked_content(infobox: HtmlElement) -> str:
    """Do a slightly better extraction of content than what lxml
    offers, in the case where it was converted from a PDF in an
    attempt to preserve layout.
    """
    # Add newlines between markedContent spans
    mcs = infobox.iterfind(".//span[@class]")
    for el in mcs:
        classes = el.get("class")
        if classes is None:
            continue
        if "markedContent" not in classes.split():
            continue
        el.tail = "\n"
    return infobox.text_content()


DATE_FORMATS = [
    "%d/%m/%Y",
    "%d/%m/%y",
    "%d-%m-%Y",
    "%d-%m-%y",
    "%d de %b de %Y",
    "%d – %m – %y",
]


def crawl_infobox(context: Context, person: Entity, infobox: HtmlElement):
    """Do a best-effort extraction of some facts from the text of an
    infobox with a member's CV."""
    text = extract_marked_content(infobox)
    m = BIRTHDATE.search(text)
    if m:
        birthdate = m.group(1)
        birthdate = WS.sub(" ", birthdate)
        birthdate = birthdate.strip(" .;,")
        context.log.debug(f"Birthdate: {birthdate}")
        person.add(
            "birthDate",
            h.parse_date(birthdate, DATE_FORMATS),
        )
    m = BIRTHPLACE.search(text)
    if m:
        birthplace = m.group(1)
        birthplace = WS.sub(" ", birthplace)
        birthplace = birthplace.strip(" .;,")
        context.log.debug(f"Place of birth: {birthplace}")
        person.add("birthPlace", birthplace)
    m = IDENTITY.search(text)
    if m:
        identity = m.group(1)
        identity = identity.strip(" .;,")
        context.log.debug(f"ID Number: {identity}")
        h.make_identification(context, person, identity)


def crawl_member_page(context: Context, person: Entity, name: str, href: str):
    """Attempt to extract information from a member's individual
    page."""
    context.log.debug(f"Fetching page for {name} from {href}")
    try:
        page = context.fetch_html(href, cache_days=1)
    except Exception as err:
        context.log.info(f"Exception when fetching {href}: {err}")
        return
    # Try to find a CV (not always present)
    tabs = page.find(".//ul[@uk-tab]")
    if tabs is None:
        context.log.debug(f"No info tabs found for {name}")
        return
    # Find the index of the CV
    for cv_idx, el in enumerate(tabs.iterfind(".//a")):
        if el.text is None:
            context.log.error(f"No text in tab {cv_idx}")
            continue
        if el.text.strip().lower() == "curriculo":
            context.log.debug(f"CV found for {name} in tab {cv_idx}")
            break
    else:
        context.log.debug(f"No CV found for {name}")
        return
    # Now find the list of tabs and get the corresponding one
    for switcher in page.iterfind(".//ul[@class]"):
        classes = switcher.get("class")
        if classes is None:
            continue
        if "uk-switcher" in classes.split():
            break
    else:
        context.log.debug("No switcher found for {name}")
        return
    # Unfortunately... these are not formatted in any particular way.
    # Do a best-effort extraction of some facts from the plain text
    crawl_infobox(context, person, list(switcher)[cv_idx])


def crawl_member(context: Context, member_link=ElementOrTree):
    """Extract member information from individual page."""
    member_name = WS.sub(" ", member_link.text.strip())
    position = h.make_position(
        context,
        name="Member of the National Assembly of Venezuela",
        country="Venezuela",
        topics=["gov.national", "gov.legislative"],
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        context.log.debug(f"Member {member_name} is not PEP")
        return

    person = context.make("Person")
    person.id = context.make_id(member_name)
    context.log.debug(f"Unique ID {person.id}")
    h.apply_name(person, full=member_name, lang="esp")

    member_href = member_link.get("href")
    if member_href is None:
        context.log.error(f"No link found for {member_name}")
    else:
        crawl_member_page(context, person, member_name, member_href)
    person.add("sourceUrl", member_href)

    occupancy = h.make_occupancy(
        context, person, position, True, categorisation=categorisation
    )
    if occupancy is not None:
        context.emit(person, target=True)
        context.emit(position)
        context.emit(occupancy)


def crawl_members(context: Context, page: ElementOrTree):
    """Extract members from a page."""
    # Don't XPath, too much trouble (wish we had CSS selectors)
    for el in page.iterfind(".//div[@class]"):
        classes = el.get("class")
        if classes is None:
            continue
        if "text-diputado-slider" not in classes.split():
            continue
        member_link = el.find(".//a")
        if member_link is None:
            context.log.error(f"No page found in element {el}")
            continue
        crawl_member(context, member_link)


def crawl_member_list(context: Context) -> Iterator[ElementOrTree]:
    """Iterate through pages in member list from the website."""
    context.log.debug(f"Fetching front page from {context.data_url}")
    page_number = 1
    page: HtmlElement = context.fetch_html(context.data_url, cache_days=1)
    yield page
    while True:
        next_links = page.find_rel_links("next")
        if not next_links:
            context.log.debug('No rel="next" link found, stopping')
            break
        # Make sure there's a link to the next page
        for link in next_links:
            href = link.get("href")
            if href is None:
                context.log.error('Missing href for rel="next" link')
                continue
            page_queries = parse_qs(urlparse(href).query).get("page")
            if page_queries is None or len(page_queries) == 0:
                context.log.error('Missing page for rel="next" link')
                continue
            next_page = int(page_queries[0])
            if next_page == page_number + 1:
                break
        else:
            context.log.error(f"Link to {page_number + 1} not found")
        page_number = next_page
        context.log.debug(f"Fetching page {page_number} from {href}")
        page = context.fetch_html(href, cache_days=1)
        yield page


def crawl(context: Context):
    """Retrieve web pages for the National Assembly and extract
    entities for members."""
    for page in crawl_member_list(context):
        crawl_members(context, page)
