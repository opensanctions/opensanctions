from typing import Any

from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise

BASE_URL = "http://council.gov.ru"
EN_MEMBERS_URL = "http://council.gov.ru/en/structure/members/"
RU_MEMBERS_URL = "http://council.gov.ru/structure/members/"


def parse_listing(doc: HtmlElement) -> dict[str, str]:
    """Return {person_id: name} from a members listing page."""
    members: dict[str, str] = {}
    for link in doc.xpath(".//a[contains(@href, '/structure/persons/')]"):
        href: str = link.get("href", "")
        # href may be /structure/persons/12345/ or /en/structure/persons/12345/
        parts = [p for p in href.split("/") if p.isdigit()]
        if not parts:
            continue
        person_id = parts[-1]
        # join itertext() with space to avoid collapsed spans (e.g. "АбрамовИван")
        name = " ".join(" ".join(link.itertext()).split())
        if not name:
            continue
        members[person_id] = name
    return members


def parse_profile(doc: HtmlElement) -> dict[str, str | None]:
    """Extract fields from the person__additional_info block of an EN profile page."""
    result: dict[str, str | None] = {
        "constituency": None,
        "born": None,
        "took office": None,
        "term ends": None,
    }

    tops = h.xpath_elements(doc, ".//div[@class='person__additional_top']")
    if tops:
        result["constituency"] = h.element_text(tops[0]) or None

    for p in h.xpath_elements(doc, ".//div[contains(@class,'person_info_private')]//p"):
        text = h.element_text(p)
        if text.startswith("Born:"):
            result["born"] = text.removeprefix("Born:").strip()
        elif text.startswith("Took office:"):
            result["took office"] = text.removeprefix("Took office:").strip()

    # Term ends: date is the tail of the person_post_star span after the ":" character
    stars = h.xpath_elements(doc, ".//span[@class='person_post_star']")
    if stars:
        tail = (stars[0].tail or "").lstrip(":").strip()
        if tail:
            result["term ends"] = tail

    return result


def crawl_person(
    context: Context,
    position: Entity,
    person_id: str,
    en_name: str,
    ru_name: str | None,
) -> None:
    profile_url = f"{BASE_URL}/en/structure/persons/{person_id}/"
    profile_doc = zyte_api.fetch_html(
        context,
        profile_url,
        unblock_validator=".//body[not(contains(., '403 Forbidden'))]",
        html_source="httpResponseBody",
        cache_days=7,
    )
    profile = parse_profile(profile_doc)

    person = context.make("Person")
    person.id = context.make_slug(person_id)

    person.add("name", en_name, lang="eng")
    if ru_name and ru_name != en_name:
        person.add("name", ru_name, lang="rus")

    h.apply_date(person, "birthDate", profile.get("born"))

    # Russian citizenship is required to hold a seat in the Federation Council
    # per Federal Law No. 113-FZ "On the Order of Formation of the Federation Council"
    # https://www.consultant.ru/document/cons_doc_LAW_37200/
    person.add("citizenship", "ru")
    person.add("sourceUrl", profile_url)

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
        start_date=profile.get("took office"),
        end_date=profile.get("term ends"),
    )
    if occupancy is not None:
        occupancy.add("constituency", profile.get("constituency"))
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

    for person_id, en_name in en_members.items():
        ru_name = ru_members.get(person_id)
        crawl_person(context, position, person_id, en_name, ru_name)
