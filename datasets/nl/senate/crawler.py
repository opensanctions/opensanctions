import re
from typing import Optional, Tuple
from urllib.parse import urljoin

from lxml.etree import _Element
from zavod.stateful.positions import categorise

from zavod import Context
from zavod import helpers as h

RESIDENCY_REGEX = re.compile(r"Woonplaats:\s*(.*)")
DOB_REGEX = re.compile(r"Geboortedatum:\s*([\d-]+)")


def get_residency_dob(
    context: Context,
    member_el: _Element,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Extracts residency and date of birth from a member element.
    Tries regex on the combined text of child nodes. Falls back to context.lookup if missing.
    Returns a tuple: (residency, dob)
    """
    node = member_el.find(".//div[@class='cell pasfoto_tekst_k2']")
    if node is None:
        context.log.warning("No node found for residency and date of birth extraction")
        return None, None
    residency, dob = None, None
    for child in node:
        if not child.text:
            continue
        if m := DOB_REGEX.search(child.text):
            dob = m.group(1)
        elif m := RESIDENCY_REGEX.search(child.text):
            residency = m.group(1)
        else:
            context.log.warning(
                "Could not extract residency or date of birth", text=child.text
            )

    return residency, dob


def get_name_date_party(
    context: Context, doc: _Element
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    wrapper = doc.find(".//div[@id='main_content_wrapper']")
    # Get first sentence of first paragraph
    description = h.element_text(wrapper)
    first_sentence = description.split("\n", 1)[0].split(".", 1)[0].strip()
    # Lookup name, start date, and party
    details_res = context.lookup("name_date_party", first_sentence)
    if details_res and details_res.details:
        details = details_res.details[0]
        return details.get("name"), details.get("start_date"), details.get("party")
    context.log.warning(
        "Could not extract name and start date from details",
        details=first_sentence,
    )
    return None, None, None


def crawl_member(context: Context, member_el: _Element) -> None:
    # Extract residency and date of birth
    residency, dob = get_residency_dob(context, member_el)

    member_link = member_el.find("./a")
    if member_link is None or member_link.get("href") is None:
        context.log.warning("No link found for member", member_el=member_el)
        return
    url = urljoin(context.data_url, member_link.get("href"))
    doc = context.fetch_html(url, cache_days=1)
    if doc is None:
        context.log.warning(f"Failed to fetch member page: {url}")
        return
    # Extract name and start date
    name_clean, start_date, party = get_name_date_party(context, doc)

    person = context.make("Person")
    person.id = context.make_id("person", name_clean, dob, party)
    person.add("name", name_clean)
    h.apply_date(person, "birthDate", dob)
    person.add("political", party)
    person.add("address", residency)
    person.add("sourceUrl", url)
    person.add("topics", "role.pep")

    position = h.make_position(
        context,
        name="Member of the Senate of the Netherlands",
        country="nl",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
        wikidata_id="Q19305384",
    )
    categorisation = categorise(context, position, True)

    if categorisation.is_pep:
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            categorisation=categorisation,
            start_date=start_date,
        )
        if occupancy:
            context.emit(position)
            context.emit(occupancy)
            context.emit(person)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1)
    member_list_items = doc.findall(".//li[@class='persoon grid-x nowr']")
    for member_el in member_list_items:
        # We keep member and URL here since we extract data from both
        crawl_member(context, member_el)
