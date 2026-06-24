import re

from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The roster page lists the current convocation; assert it so the crawler fails loudly
# when the next convocation is seated and the page contents change.
EXPECTED_CONVOCATION = "VII convocation"
DEPUTY_RE = re.compile(r"single-deputy/(\d+)")


def parse_deputies(doc: HtmlElement) -> dict[str, str]:
    """Map each deputy's id to their name (the profile-link text) on the roster page."""
    deputies: dict[str, str] = {}
    for link in h.xpath_elements(doc, "//a[contains(@href, 'single-deputy/')]"):
        match = DEPUTY_RE.search(link.get("href") or "")
        name = h.element_text(link)
        if match is not None and len(name) > 0:
            deputies[match.group(1)] = name
    return deputies


def crawl_deputy(
    context: Context,
    deputy_id: str,
    name_tm: str,
    name_ru: str | None,
    name_en: str | None,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    person = context.make("Person")
    person.id = context.make_slug("deputy", deputy_id)
    h.apply_name(person, full=name_tm, lang="tuk")  # Turkmen Latin (authoritative)
    if name_ru is not None:
        h.apply_name(person, full=name_ru, lang="rus", alias=True)
    if name_en is not None:
        h.apply_name(person, full=name_en, lang="eng", alias=True)
    # Deputies of the Mejlis must be citizens of Turkmenistan (Constitution art. 120).
    # https://www.constituteproject.org/constitution/Turkmenistan_2016
    person.add("citizenship", "tm")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)


def fetch_roster(context: Context, lang: str) -> dict[str, str]:
    doc = context.fetch_html(context.data_url, params={"lang": lang}, cache_days=1)
    return parse_deputies(doc)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Mejlis of Turkmenistan",
        country="tm",
        topics=["gov.national", "gov.legislative"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    en_doc = context.fetch_html(context.data_url, params={"lang": "en"}, cache_days=1)
    if EXPECTED_CONVOCATION not in h.element_text(en_doc):
        raise ValueError(f"Roster no longer lists {EXPECTED_CONVOCATION!r}")
    en = parse_deputies(en_doc)
    ru = fetch_roster(context, "ru")
    deputies = fetch_roster(context, "tm")

    for deputy_id, name_tm in deputies.items():
        crawl_deputy(
            context,
            deputy_id,
            name_tm,
            ru.get(deputy_id),
            en.get(deputy_id),
            position,
            categorisation,
        )
