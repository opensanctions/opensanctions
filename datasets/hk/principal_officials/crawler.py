import re
from normality import collapse_spaces

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise
from zavod.util import Element


def get_element_text(
    doc: Element, xpath_value: str, to_remove: list[str] = [], position: int = 0
) -> str | None:
    elements = h.xpath_elements(doc, xpath_value)
    element_text = h.element_text(elements[position]) if elements else ""

    for string in to_remove:
        element_text = element_text.replace(string, "")

    return collapse_spaces(element_text.strip())


def crawl_members(context: Context, section: str, elem: Element) -> None:
    url = elem.get("href")
    assert url is not None

    doc = context.fetch_html(url, cache_days=1)

    member_header = get_element_text(doc, '//h1[contains(@class,"title")]')
    assert member_header is not None
    member_desc = get_element_text(doc, '//section[@class="blockItem"]')

    person_name = member_header.split(",")[0]
    person_title = person_name.split()[0]
    person_name = " ".join(person_name.split()[1:])

    person = context.make("Person")
    person.id = context.make_slug(person_name)
    person.add("name", person_name)
    person.add("title", person_title)
    person.add("summary", member_header)
    person.add("biography", member_desc)
    person.add("sourceUrl", url)
    # various positions
    person.add("country", "hk")

    position_name = ",".join(member_header.split(",")[1:])
    position_name = re.sub(
        r"\b[A-Z]{2,5}(?:,\s*)?\b", "", position_name
    )  # remove abbreviations
    position_name = f"{position_name.strip()}"
    position = h.make_position(
        context, position_name, country="hk", description=section
    )

    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )

    # assert added during typechecker fixes to not change behavior; may not reflect reality
    assert occupancy is not None
    context.emit(person)
    context.emit(position)
    context.emit(occupancy)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    for section in h.xpath_elements(doc, '//section[@class="blockItem"]'):
        section_name = h.element_text(h.xpath_element(section, "./h3"))
        for elem in h.xpath_elements(section, ".//p//a"):
            crawl_members(context, section_name, elem)
