from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise

from normality import collapse_spaces
from xml.etree import ElementTree
import re


def get_element_text(doc: ElementTree, xpath_value: str, to_remove=[], position=0):
    element_tag = doc.xpath(xpath_value)
    element_text = element_tag[position].text_content() if element_tag else ""

    for string in to_remove:
        element_text = element_text.replace(string, "")

    return collapse_spaces( element_text.strip() ) 


def crawl_members(context: Context, section: str, elem: ElementTree):

    url = elem.get("href")

    doc = context.fetch_html(url, cache_days=1)

    member_header = get_element_text(doc, '//h1[contains(@class,"title")]')
    member_desc = get_element_text(doc, '//section[@class="blockItem"]')

    person_name = member_header.split(",")[0]
    person_title = person_name.split()[0]
    person_name = " ".join(person_name.split()[1:])

    person = context.make("Person")
    person.id = context.make_slug(person_name)
    person.add("name", person_name)
    person.add("title", person_title)
    person.add("summary", member_header)
    person.add("description", member_desc)
    person.add("sourceUrl", url)

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
        no_end_implies_current=True,
    )

    context.emit(person, target=True)
    context.emit(position)
    context.emit(occupancy)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    doc.make_links_absolute(context.data_url)

    for section in doc.xpath('//section[@class="blockItem"]'):

        section_name = section.xpath("./h3")[0].text_content()
        officials = section.xpath(".//p//a")

        for elem in officials:
            crawl_members(context, section_name, elem)
