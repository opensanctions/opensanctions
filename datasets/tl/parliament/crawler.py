import re

from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

from zavod import Context
from zavod import helpers as h

PROFILE_URL = "https://www.parlamento.tl/deputados/"
MEMBERS_URL = "https://www.parlamento.tl/membros"
# Nicknames are quoted inline, e.g. 'Maria Rosa da Câmara "Bisoi"'.
NICKNAME_RE = re.compile(r'\s*"([^"]+)"')


def parse_details(profile_doc: Element, context: Context) -> dict[str, str]:
    details: dict[str, str] = {}
    for label in h.xpath_elements(profile_doc, '//p/strong[contains(., ":")]'):
        field = context.lookup_value(
            "details", h.element_text(label), warn_unmatched=True
        )
        if field is None:
            continue
        # Most labels sit in their own paragraph; a few carry the value inline.
        value = (label.tail or "").strip()
        if not value:
            # An empty field is followed directly by the next label, not a value.
            next_paragraphs = h.xpath_elements(
                label, "../following-sibling::p[1][not(strong)]"
            )
            value = h.element_text(next_paragraphs[0]) if next_paragraphs else ""
        details[field] = value
    return details


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    member_link: Element,
    party: str | None,
) -> None:
    profile_url = h.xpath_string(member_link, "./@href")
    raw_name = h.element_text(member_link)

    person = context.make("Person")
    person.id = context.make_slug(raw_name)
    person.add("name", NICKNAME_RE.sub("", raw_name), lang="por")
    person.add("alias", NICKNAME_RE.findall(raw_name))
    person.add("political", party)
    # Every citizen over seventeen has the right to be elected (Constitution of the
    # RDTL, Section 47(1)). https://www.constituteproject.org/constitution/East_Timor_2002
    person.add("citizenship", "tl")
    person.add("sourceUrl", profile_url)

    profile_doc = context.fetch_html(profile_url, cache_days=7)
    details = parse_details(profile_doc, context)
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
    members_doc = context.fetch_html(MEMBERS_URL, cache_days=1)
    term_option = h.xpath_element(
        members_doc, '//select[@name="legislatura"]/option[@selected]'
    )
    context.lookup("term", h.element_text(term_option), warn_unmatched=True)

    parties_doc = context.fetch_html(context.data_url, cache_days=1)
    # Each party card opens a modal headed e.g. "CNRT - Deputados" listing its members.
    for party_modal in h.xpath_elements(
        parties_doc, '//div[starts-with(@id, "modal-")]'
    ):
        party_heading = h.element_text(h.xpath_element(party_modal, ".//h3"))
        party = context.lookup_value("party", party_heading, warn_unmatched=True)
        member_links = h.xpath_elements(
            party_modal, f'.//a[starts-with(@href, "{PROFILE_URL}")]'
        )
        for member_link in member_links:
            crawl_member(context, position, categorisation, member_link, party)
