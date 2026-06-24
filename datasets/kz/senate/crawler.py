import re

from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

SITE = "https://senate.parlam.kz"

# Biographies state the birth date shortly after a "родился/родилась" or "дата
# рождения" marker, as either "<d> <month genitive> <year>" or "<dd>.<mm>.<yyyy>".
RU_MONTHS = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}
BIRTH_MARKER = re.compile(r"родил(?:ся|ась)|дата рождения", re.IGNORECASE)
TEXT_DATE = re.compile(r"(\d{1,2})\s+([а-яё]+)\s+(\d{4})")
NUM_DATE = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})")


def parse_cards(doc: HtmlElement) -> dict[str, str]:
    """Map each senator's profile id to their full name on a deputies list page."""
    cards: dict[str, str] = {}
    for card in h.xpath_elements(doc, "//a[contains(@class, 'person-card')]"):
        href = card.get("href")
        match = re.search(r"/blog/(\d+)/", href or "")
        if match is None:
            continue
        names = h.xpath_elements(
            card, ".//*[contains(@class, 'person-card--full-name')]"
        )
        cards[match.group(1)] = h.element_text(names[0])
    return cards


def extract_birth_date(doc: HtmlElement) -> str | None:
    text = re.sub(r"\s+", " ", doc.text_content())
    marker = BIRTH_MARKER.search(text)
    if marker is None:
        return None
    # The birth date is the first date right after the marker (a birthplace may sit
    # between them, so allow a short window).
    window = text[marker.start() : marker.start() + 80]
    num = NUM_DATE.search(window)
    if num is not None:
        day, month, year = num.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"
    txt = TEXT_DATE.search(window)
    if txt is not None:
        day, month_name, year = txt.groups()
        month = RU_MONTHS.get(month_name)
        if month is not None:
            return f"{year}-{month:02d}-{int(day):02d}"
    return None


def crawl_senator(
    context: Context,
    senator_id: str,
    name_ru: str,
    name_kk: str | None,
    name_en: str | None,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    person = context.make("Person")
    person.id = context.make_slug("senator", senator_id)
    h.apply_name(person, full=name_ru, lang="rus")
    if name_kk is not None:
        h.apply_name(person, full=name_kk, lang="kaz", alias=True)
    if name_en is not None:
        h.apply_name(person, full=name_en, lang="eng", alias=True)

    bio = context.fetch_html(f"{SITE}/ru-RU/blog/{senator_id}/biography", cache_days=7)
    h.apply_date(person, "birthDate", extract_birth_date(bio))
    # Senators must be citizens of Kazakhstan (Constitution art. 51(4)).
    # https://www.constituteproject.org/constitution/Kazakhstan_2017
    person.add("citizenship", "kz")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Senate of the Parliament of Kazakhstan",
        country="kz",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21295141",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    ru = parse_cards(context.fetch_html(context.data_url, cache_days=1))
    kk = parse_cards(
        context.fetch_html(context.data_url.replace("/ru-RU/", "/kk-KZ/"), cache_days=1)
    )
    en = parse_cards(
        context.fetch_html(context.data_url.replace("/ru-RU/", "/en-US/"), cache_days=1)
    )
    for senator_id, name_ru in ru.items():
        crawl_senator(
            context,
            senator_id,
            name_ru,
            kk.get(senator_id),
            en.get(senator_id),
            position,
            categorisation,
        )
