from itertools import count
from urllib.parse import urljoin

from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise


def parse_profile(profile: HtmlElement) -> dict[str, str]:
    """Read the label/value table on an MP profile page into a dict."""
    detail = h.xpath_elements(profile, "//div[@class='met-profile_user-detail']")
    if len(detail) != 1:
        return {}
    fields: dict[str, str] = {}
    for row in h.xpath_elements(detail[0], ".//table//tr"):
        cells = h.xpath_elements(row, "./td")
        if len(cells) != 2:
            continue
        label = h.element_text(cells[0]).rstrip(":").strip().upper()
        value = h.element_text(cells[1])
        # "N/A" marks a field that is not applicable to this member.
        if len(label) == 0 or len(value) == 0 or value.upper() == "N/A":
            continue
        fields[label] = value
    return fields


def crawl_member(
    context: Context,
    url: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    profile = context.fetch_html(url, cache_days=1)
    headings = h.xpath_elements(profile, "//h5[@class='met-user-name']")
    if len(headings) != 1:
        context.log.warning("Cannot find member name", url=url)
        return
    name = h.element_text(headings[0])
    if len(name) == 0:
        context.log.warning("Empty member name", url=url)
        return

    fields = parse_profile(profile)
    district = fields.get("DISTRICT")
    constituency = fields.get("CONSTITUENCY")

    person = context.make("Person")
    person.id = context.make_id(name, district)

    # names have prefix - using name prefix strip framework in meta
    clean_name = h.strip_name_titles(context, name)
    h.apply_name(person, full=clean_name, lang="eng")

    person.add("sourceUrl", url)
    person.add("email", fields.get("EMAIL"))
    person.add("political", fields.get("POLITICAL PARTY"))
    dob = fields.get("DATE OF BIRTH")
    h.apply_date(person, "birthDate", dob)
    # Members of Parliament must be citizens of Uganda (Constitution art. 80(1)(a)).
    # https://www.constituteproject.org/constitution/Uganda_2017
    person.add("citizenship", "ug")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("constituency", constituency)
    occupancy.add("constituency", district)
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Parliament of Uganda",
        country="ug",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21296005",
        lang="eng",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    context.emit(position)

    seen: set[str] = set()
    for page in count(1):
        doc = context.fetch_html(context.data_url, params={"page": page}, cache_days=1)
        cards = h.xpath_elements(doc, "//a[contains(@href, '/home/mp/')]")
        card_count = 0
        for card in cards:
            href = card.get("href")
            if href is None or href in seen:
                continue
            seen.add(href)
            card_count += 1
            crawl_member(
                context, urljoin(context.data_url, href), position, categorisation
            )
        if card_count == 0:
            break
