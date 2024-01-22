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


def crawl_infobox(context: Context, person: Entity, infobox: HtmlElement):
    """Do a best-effort extraction of some facts from the text of an
    infobox with a member's CV."""
    text = infobox.text_content()
    m = BIRTHDATE.search(text)
    if m:
        birthdate = m.group(1)
        birthdate = WS.sub(" ", birthdate)
        birthdate = birthdate.strip(" .;,")
        context.log.info(f"Birthdate: {birthdate}")
        person.add(
            "birthDate",
            h.parse_date(
                birthdate,
                (
                    "%d/%m/%Y",
                    "%d/%m/%y",
                    "%d-%m-%Y",
                    "%d-%m-%y",
                    "%d de %b de %Y",
                    "%d – %m – %y",
                ),
            ),
        )
    m = BIRTHPLACE.search(text)
    if m:
        birthplace = m.group(1)
        birthplace = WS.sub(" ", birthplace)
        birthplace = birthplace.strip(" .;,")
        context.log.info(f"Place of birth: {birthplace}")
        person.add("birthPlace", birthplace)
    m = IDENTITY.search(text)
    if m:
        identity = m.group(1)
        identity = identity.strip(" .;,")
        context.log.info(f"ID Number: {identity}")
        h.make_identification(context, person, identity)


def crawl_member_page(context: Context, person: Entity, name: str, href: str):
    """Attempt to extract information from a member's individual
    page."""
    context.log.info(f"Fetching page for {name} from {href}")
    try:
        page = context.fetch_html(href, cache_days=1)
    except Exception:
        context.log.error(f"Exception when fetching {href}")
        return
    # Try to find a CV (not always present)
    tabs = page.find(".//ul[@uk-tab]")
    if tabs is None:
        context.log.info(f"No info tabs found for {name}")
        return
    # Find the index of the CV
    for cv_idx, el in enumerate(tabs.iterfind(".//a")):
        if el.text is None:
            context.log.error(f"No text in tab {cv_idx}")
            continue
        if el.text.strip().lower() == "curriculo":
            context.log.info(f"CV found for {name} in tab {cv_idx}")
            break
    else:
        context.log.info(f"No CV found for {name}")
        return
    # Now find the list of tabs and get the corresponding one
    for switcher in page.iterfind(".//ul[@class]"):
        classes = switcher.get("class")
        if classes is None:
            continue
        if "uk-switcher" in classes.split():
            break
    else:
        context.log.info("No switcher found for {name}")
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
        context.log.info(f"Member {member_name} is not PEP")
        return

    person = context.make("Person")
    person.id = context.make_id(member_name)
    context.log.info(f"Unique ID {person.id}")
    h.apply_name(person, full=member_name, lang="esp")

    member_href = member_link.get("href")
    if member_href is None:
        context.log.error(f"No link found for {member_name}")
    else:
        crawl_member_page(context, person, member_name, member_href)

    context.emit(person, target=True)
    context.emit(position)
    occupancy = h.make_occupancy(
        context, person, position, True, categorisation=categorisation
    )
    if occupancy is not None:
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
    assert context.dataset.data is not None
    assert context.dataset.data.url is not None
    context.log.info(f"Fetching front page from {context.dataset.data.url}")
    page_number = 1
    page: HtmlElement = context.fetch_html(context.dataset.data.url, cache_days=1)
    yield page
    while True:
        next_links = page.find_rel_links("next")
        if not next_links:
            context.log.info('No rel="next" link found, stopping')
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
        context.log.info(f"Fetching page {page_number} from {href}")
        page = context.fetch_html(href, cache_days=1)
        yield page


def crawl(context: Context):
    """Retrieve web pages for the National Assembly and extract
    entities for members."""
    for page in crawl_member_list(context):
        crawl_members(context, page)
