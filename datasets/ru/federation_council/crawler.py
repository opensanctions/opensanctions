from typing import Any

from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

BASE_URL = "http://council.gov.ru"
EN_MEMBERS_URL = "http://council.gov.ru/en/structure/members/"
RU_MEMBERS_URL = "http://council.gov.ru/structure/members/"


def parse_listing(doc: HtmlElement) -> dict[str, dict[str, Any]]:
    """Return {person_id: {"name": ..., "constituency": ...}} from a members listing page."""
    members: dict[str, dict[str, Any]] = {}
    for link in doc.xpath(".//a[contains(@href, '/structure/persons/')]"):
        href: str = link.get("href", "")
        # href may be /structure/persons/12345/ or /en/structure/persons/12345/
        parts = [p for p in href.split("/") if p.isdigit()]
        if not parts:
            continue
        person_id = parts[-1]
        name = " ".join(link.itertext()).strip()
        name = " ".join(name.split())  # normalise internal whitespace
        if not name:
            continue
        # Constituency is often in a sibling element; try the parent li/div text
        constituency: str | None = None
        parent = link.getparent()
        if parent is not None:
            for sibling in parent:
                if sibling is link:
                    continue
                text = h.element_text(sibling)
                if text and len(text) > 2:
                    constituency = text
                    break
        members[person_id] = {"name": name, "constituency": constituency}
    return members


def crawl_person(
    context: Context,
    position: Entity,
    person_id: str,
    en_data: dict[str, Any],
    ru_name: str | None,
) -> None:
    person = context.make("Person")
    person.id = context.make_slug(person_id)

    en_name: str = en_data["name"]
    constituency: str | None = en_data.get("constituency")

    # Add Latin-script name from English listing
    person.add("name", en_name, lang="eng")

    # Add Cyrillic name from Russian listing as additional name variant
    if ru_name and ru_name != en_name:
        person.add("name", ru_name, lang="rus")

    # Russian citizenship is required to hold a seat in the Federation Council
    # per Federal Law No. 113-FZ "On the Order of Formation of the Federation Council"
    # https://www.consultant.ru/document/cons_doc_LAW_37200/
    person.add("citizenship", "ru")
    person.add("sourceUrl", f"{BASE_URL}/en/structure/persons/{person_id}/")

    # IMPORTANT: all person props must be set before make_occupancy;
    # categorise() is called here, adjacent to make_occupancy()
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is not None:
        if constituency:
            occupancy.add("constituency", constituency)
        context.emit(occupancy)
        # emit person AFTER make_occupancy — it adds role.pep to person.topics
        context.emit(person)


def crawl(context: Context) -> None:
    # Q4516946 — member of the upper house of the Federal Assembly of Russia
    position = h.make_position(
        context,
        name="Member of the Federation Council of Russia",
        country="ru",
        wikidata_id="Q4516946",
        topics=["gov.national", "gov.legislative"],
    )
    context.emit(position)

    en_doc = context.fetch_html(EN_MEMBERS_URL, cache_days=1)
    ru_doc = context.fetch_html(RU_MEMBERS_URL, cache_days=1)

    en_members = parse_listing(en_doc)
    ru_members = parse_listing(ru_doc)

    if not en_members:
        raise ValueError(
            "No members found on EN listing page — the site structure may have changed"
        )

    for person_id, en_data in en_members.items():
        ru_name = ru_members.get(person_id, {}).get("name") if ru_members else None
        crawl_person(context, position, person_id, en_data, ru_name)
