from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise

BASE_URL = "http://council.gov.ru"
EN_MEMBERS_URL = "http://council.gov.ru/en/structure/members/"


def crawl_person(
    context: Context,
    position: Entity,
    profile_url: str,
) -> None:
    name_xpath = ".//h2[@class='senators_title']//span"
    doc: HtmlElement = zyte_api.fetch_html(
        context,
        profile_url,
        unblock_validator=name_xpath,
        html_source="httpResponseBody",
    )

    person_id = profile_url.rstrip("/").rsplit("/", 1)[-1]
    person = context.make("Person")
    person.id = context.make_slug(person_id)
    name = h.element_text(h.xpath_element(doc, name_xpath))
    person.add("name", name, lang="eng")

    info_paras = h.xpath_elements(
        doc, ".//div[contains(@class,'person_info_private')]//p"
    )
    for p in info_paras:
        text = h.element_text(p)
        if text.startswith("Born:"):
            h.apply_date(person, "birthDate", text.removeprefix("Born:").strip())

    # Russian citizenship is required to hold a seat in the Federation Council
    # per Federal Law No. 113-FZ "On the Order of Formation of the Federation Council"
    # https://www.consultant.ru/document/cons_doc_LAW_37200/
    person.add("citizenship", "ru")
    person.add("sourceUrl", profile_url)

    # IMPORTANT: all person props must be set before make_occupancy
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return

    took_office: str | None = None
    term_ends: str | None = None
    for p in info_paras:
        text = h.element_text(p)
        if text.startswith("Took office:"):
            took_office = text.removeprefix("Took office:").strip()

    starts = h.xpath_elements(doc, ".//span[@class='person_post_star']")
    if starts:
        tail = (starts[0].tail or "").lstrip(":").strip()
        if tail:
            term_ends = tail

    constituency: str | None = None
    tops = h.xpath_elements(doc, ".//div[@class='person__additional_top']")
    if tops:
        constituency = h.element_text(tops[0]) or None

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        start_date=took_office,
        end_date=term_ends,
    )
    if occupancy is not None:
        occupancy.add("constituency", constituency)

        context.emit(occupancy)
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

    doc = context.fetch_html(EN_MEMBERS_URL, cache_days=1)
    links = h.xpath_elements(doc, ".//a[contains(@href, '/structure/persons/')]")
    if not links:
        raise ValueError(
            "No member links found on listing page — the site structure may have changed"
        )
    seen: set[str] = set()
    for link in links:
        href: str = link.get("href", "")
        parts = [p for p in href.split("/") if p.isdigit()]
        if not parts or parts[-1] in seen:
            continue
        seen.add(parts[-1])
        crawl_person(context, position, f"{BASE_URL}/en/structure/persons/{parts[-1]}/")
