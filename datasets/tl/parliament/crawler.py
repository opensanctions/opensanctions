import re

from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

from zavod import Context
from zavod import helpers as h

PROFILE_URL = "https://www.parlamento.tl/deputados/"
MEMBERS_URL = "https://www.parlamento.tl/membros"
# Each party's members are listed in a modal headed e.g. "CNRT - Deputados".
HEADING_SUFFIX = " - Deputados"
# Nicknames are quoted inline, e.g. 'Maria Rosa da Câmara "Bisoi"'.
NICKNAME_RE = re.compile(r'\s*"([^"]+)"')


def parse_details(doc: Element, context: Context) -> dict[str, str]:
    details: dict[str, str] = {}
    for strong in h.xpath_elements(doc, '//p/strong[contains(., ":")]'):
        key = context.lookup_value(
            "details", h.element_text(strong), warn_unmatched=True
        )
        if key is None:
            continue
        # Most labels sit in their own paragraph; a few carry the value inline.
        value = (strong.tail or "").strip()
        if not value:
            # An empty field is followed directly by the next label, not a value.
            siblings = h.xpath_elements(
                strong, "../following-sibling::p[1][not(strong)]"
            )
            value = h.element_text(siblings[0]) if siblings else ""
        details[key] = value
    return details


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    link: Element,
    party: str,
) -> None:
    url = h.xpath_string(link, "./@href")
    raw_name = h.element_text(link)

    person = context.make("Person")
    person.id = context.make_slug(raw_name)
    person.add("name", NICKNAME_RE.sub("", raw_name), lang="por")
    person.add("alias", NICKNAME_RE.findall(raw_name))
    person.add("political", party)
    # Every citizen over seventeen has the right to be elected (Constitution of the
    # RDTL, Section 47(1)). https://www.constituteproject.org/constitution/East_Timor_2002
    person.add("citizenship", "tl")
    person.add("sourceUrl", url)

    doc = context.fetch_html(url, cache_days=7)
    details = parse_details(doc, context)
    h.apply_date(person, "birthDate", details.get("birth_date"))
    person.add("birthPlace", details.get("birth_place"))
    person.add("address", details.get("municipality"))

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=details.get("start_date"),
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Parliament of Timor-Leste",
        country="tl",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q19966812",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # /bancadas doesn't name the term it lists, but the /membros term selector
    # defaults to the sitting one, so an unmatched label means a new parliament.
    members = context.fetch_html(MEMBERS_URL, cache_days=1)
    term = h.xpath_element(members, '//select[@name="legislatura"]/option[@selected]')
    context.lookup("term", h.element_text(term), warn_unmatched=True)

    doc = context.fetch_html(context.data_url, cache_days=1)
    for modal in h.xpath_elements(doc, '//div[starts-with(@id, "modal-")]'):
        heading = h.xpath_string(
            modal, f'.//h3[contains(text(), "{HEADING_SUFFIX}")]/text()'
        )
        party = heading.strip().removesuffix(HEADING_SUFFIX)
        links = h.xpath_elements(modal, f'.//a[starts-with(@href, "{PROFILE_URL}")]')
        for link in links:
            crawl_member(context, position, categorisation, link, party)
