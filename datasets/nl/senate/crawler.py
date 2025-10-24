import re
from typing import Optional, Tuple
from urllib.parse import urljoin

from lxml.etree import _Element
from zavod.stateful.positions import categorise

from zavod import Context
from zavod import helpers as h

# Single regex for both fields (covers 95% of cases)
RESIDENCY_DOB_REGEX = re.compile(r"Woonplaats:\s*(.*?)\s*Geboortedatum:\s*([\d-]+)")


def get_residency_dob(
    context: Context,
    member: _Element,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Extracts residency and date of birth from a member element.
    Tries regex on the combined text of child nodes. Falls back to context.lookup if missing.
    Returns a tuple: (residency, dob)
    """
    node = member.find(".//div[@class='cell pasfoto_tekst_k2']")
    if node is None:
        context.log.warning("No node found for residency and date of birth extraction")
        return None, None
    # Combine all child text
    full_text = " ".join(child.text.strip() for child in node if child.text)
    # Attempt regex extraction first
    match = RESIDENCY_DOB_REGEX.search(full_text)
    if match:
        residency = match.group(1).strip()
        dob = match.group(2).strip()
        return residency, dob
    # Fallback to context.lookup
    details_res = context.lookup("residency_dob", full_text)
    if details_res:
        details = details_res.details[0]
        return details.get("residency"), details.get("dob")
    context.log.warning(
        "Could not extract residency and date of birth",
        full_text=full_text,
    )
    return None, None


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


def crawl_member(context: Context, member: _Element) -> None:
    a_tag = member.find("./a")
    url = urljoin(context.data_url, a_tag.get("href"))
    doc = context.fetch_html(url, cache_days=3)
    if doc is None:
        context.log.warning(f"Failed to fetch member page: {url}")
        return
    # Extract name and start date
    name_clean, start_date, party = get_name_date_party(context, doc)
    # Extract residency and date of birth
    residency, dob = get_residency_dob(context, member)

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
    doc = context.fetch_html(context.data_url, cache_days=3)
    members = doc.findall(".//li[@class='persoon grid-x nowr']")
    if not members:
        context.log.warning("No members found on the page")
        return
    for member in members:
        crawl_member(context, member)
