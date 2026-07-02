import re

from rigour.territories import get_ftm_countries

from zavod import Context
from zavod import helpers as h
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


def crawl_person(context: Context, person_id: str, kind: str) -> None:
    url = f"{BASE_URL}/catalog/persons/{person_id}/{kind}"
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

    foreign_country = foreign_country_match(title)
    if foreign_country is not None:
        context.log.info(
            "Skipping non-Russian position",
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

    # IMPORTANT: all person props must be set before make_occupancy/categorise.
    position = h.make_position(
        context,
        name=title,
        country="ru",
    )
    # Mixed source: government appointees, elected officials, business executives
    # and (filtered out above) foreign leaders all share the one position field.
    categorisation = categorise(context, position, default_is_pep=None)
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
    link_pattern = re.compile(r"/catalog/persons/(\d+)/(biography|events)")

    people: dict[str, str] = {}
    for link in h.xpath_elements(doc, ".//a[contains(@href, '/catalog/persons/')]"):
        href = link.get("href", "")
        match = link_pattern.search(href)
        if not match:
            continue
        person_id, kind = match.group(1), match.group(2)
        people.setdefault(person_id, kind)

    for person_id, kind in people.items():
        crawl_person(context, person_id, kind)
