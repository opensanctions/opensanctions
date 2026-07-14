import re

from lxml import etree
from rigour.territories import get_ftm_countries

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise

BASE_URL = "http://en.kremlin.ru"
# A former/deceased person's last position is suffixed with the years they held it,
# e.g. "Adviser to the President of the Russian Federation (2009 - 2010)".
DATE_RANGE = re.compile(r"^(.*?)\s*\((\d{4})\s*-\s*(\d{4})\)\s*$")

FOREIGN_COUNTRY_NAMES = sorted(
    {
        country.name
        for country in get_ftm_countries()
        if country.code != "ru"
        and country.is_country
        and not country.is_historical
        and country.name
    },
    key=len,
    reverse=True,
)


def foreign_country_match(title: str) -> str | None:
    for name in FOREIGN_COUNTRY_NAMES:
        if re.search(rf"\b{re.escape(name)}\b", title, re.IGNORECASE):
            return name
    return None


def parse_birth(
    context: Context, person: Entity, doc: etree._Element, url: str
) -> None:
    """Apply birth date and place from the biography's leading "Born ..." line.

    Birth details are only ever available as free-text prose, so rather than
    parse them in code we route the raw line through the ``birth`` lookup, which
    maps each exact line to an ISO date and cleaned place name(s). Unmatched
    lines are warned about and left unset, never guessed at.
    """
    born_lines = [
        text
        for el in h.xpath_elements(doc, ".//dl[@class='separate_dates']//dd")
        if (text := h.element_text(el).replace("\xa0", " ").strip()).startswith("Born")
    ]
    if len(born_lines) == 0:
        context.log.warning("No birth line found in biography", url=url)
        return
    if len(born_lines) > 1:
        context.log.warning("Multiple birth lines found in biography", url=url)
        return

    result = context.lookup("birth", born_lines[0], warn_unmatched=True)
    if result is None:
        return
    h.apply_date(person, "birthDate", result.birth_date)
    for place in result.birth_place or []:
        person.add("birthPlace", place)


def crawl_person(context: Context, person_id: str) -> None:
    url = f"{BASE_URL}/catalog/persons/{person_id}/biography"
    name_xpath = ".//*[@itemprop='familyName']"
    doc = zyte_api.fetch_html(
        context,
        url,
        unblock_validator=name_xpath,
        html_source="httpResponseBody",
        cache_days=7,
    )

    family_name = h.element_text(h.xpath_element(doc, name_xpath))
    given_name = h.element_text(h.xpath_element(doc, ".//*[@itemprop='givenName']"))

    title_elements = h.xpath_elements(doc, ".//*[@itemprop='jobTitle']")
    if len(title_elements) == 0:
        context.log.info(
            "No position listed, skipping",
            url=url,
            name=f"{given_name} {family_name}",
        )
        return
    if len(title_elements) > 1:
        context.log.warning("Multiple job titles found", url=url)
        return
    raw_title = h.element_text(title_elements[0]).replace("\xa0", " ").strip()

    range_match = DATE_RANGE.match(raw_title)
    if range_match:
        title, start_date, end_date = (
            range_match.group(1),
            range_match.group(2),
            range_match.group(3),
        )
    else:
        title, start_date, end_date = raw_title, None, None

    # Only Russian-state figures get a narrative biography here; foreign leaders
    # get an events-only page and are never crawled. A foreign country in the
    # title of a biography page therefore breaks that assumption: warn and skip
    # rather than emit the person mislabelled as Russian.
    foreign_country = foreign_country_match(title)
    if foreign_country is not None:
        context.log.warning(
            "Foreign country in biography title, skipping",
            url=url,
            title=title,
            country=foreign_country,
        )
        return

    person = context.make("Person")
    person.id = context.make_slug(person_id)
    h.apply_name(person, given_name=given_name, last_name=family_name, lang="eng")
    # Positions here are Presidential Executive Office appointees, civil servants and
    # security service officials, not directly elected, so `country` rather than
    # `citizenship` (see zavod/docs/peps.md, "Properties to capture").
    person.add("country", "ru")
    person.add("sourceUrl", url)

    parse_birth(context, person, doc, url)

    # IMPORTANT: all person props must be set before make_occupancy/categorise.
    position = h.make_position(
        context,
        name=title,
        country="ru",
    )
    # A biography page can still describe a former official or someone whose most
    # recent role is non-state (e.g. an international sports body), so PEP status
    # is decided manually in the review UI rather than assumed.
    categorisation = categorise(context, position)
    if categorisation.is_pep is not True:
        return

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        start_date=start_date,
        end_date=end_date,
        # An undated title reliably means "currently held" here: departed and
        # deceased officials get either a trailing year range or no title at all.
        no_end_implies_current=True,
    )
    if occupancy is not None:
        context.emit(person)
        context.emit(position)
        context.emit(occupancy)


def crawl(context: Context) -> None:
    # Plain requests get connection-timed-out/blocked by this site, even for the
    # listing page, so route through Zyte like the per-person pages below.
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=".//a[contains(@href, '/catalog/persons/')]",
        html_source="httpResponseBody",
        cache_days=1,
    )
    # Only crawl people the Kremlin has written a narrative biography for: the
    # directory links them via `/biography`, while everyone else (foreign leaders,
    # people who merely appear in event listings) gets an `/events` link with no
    # date/place of birth. Biography pages are both the reliable Russian-official
    # signal and the only pages carrying the biographical detail we want.
    link_pattern = re.compile(r"/catalog/persons/(\d+)/biography")

    person_ids: set[str] = set()
    for link in h.xpath_elements(doc, ".//a[contains(@href, '/catalog/persons/')]"):
        href = link.get("href", "")
        match = link_pattern.search(href)
        if match is None:
            continue
        person_ids.add(match.group(1))

    for person_id in person_ids:
        crawl_person(context, person_id)
