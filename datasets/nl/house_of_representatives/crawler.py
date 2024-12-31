from urllib.parse import urlencode, urljoin
from lxml.etree import _Element
from lxml.html import document_fromstring
import re

from zavod.shed.zyte_api import fetch_html, fetch_json
from zavod import Context
from zavod.entity import Entity
from zavod.helpers import make_position, make_occupancy
from zavod.logic.pep import categorise
from zavod import helpers as h


REGEX_BIRTH_PLACE_AND_DATE = re.compile(
    "is geboren in (?P<birthplace>.+) op (?P<birthdate>.+\d{4})(\.| en woont in)"
)


def unblock_validator(doc):
    return doc.find('.//main[@class="o-main"]/article/section[2]') is not None


def crawl_person(context: Context, element: _Element, position: Entity):
    anchor = element.find(".//a")
    source_url = urljoin(context.data_url, anchor.get("href"))
    section_xpath = './/main[@class="o-main"]/article/section[2]'
    doc = fetch_html(context, source_url, section_xpath, cache_days=1)
    section = doc.find(section_xpath)

    # Not a lot of semantic selection information in the HTML so the correctness of
    # the selectors depends very much on hard requirements on this matching.
    match = re.search(REGEX_BIRTH_PLACE_AND_DATE, section.findtext(".//p"))
    birth_date = match.group("birthdate")
    name = section.findtext(".//h1")

    person = context.make("Person")
    person.id = context.make_id(name, birth_date)
    person.add("name", name)

    person.add("birthPlace", match.group("birthplace"))
    h.apply_date(person, "birthDate", birth_date)
    person.add("topics", "role.pep")
    person.add("sourceUrl", source_url)

    occupancy = make_occupancy(
        context,
        person,
        position,
        end_date=None,
        no_end_implies_current=True,
    )

    context.emit(occupancy)
    context.emit(person, target=True)


def crawl(context: Context):
    position = make_position(
        context,
        name="Member of the House of Representatives",
        country="nl",
        summary="Member of the lower house of the bicameral parliament of the Netherlands, the States General",
    )
    categorise(context, position, True)
    context.emit(position)

    view_dom_id = "whatever"

    params = {
        "view_name": "members_of_parliament",
        "view_display_id": "page_all_members",
        "view_dom_id": view_dom_id,
    }

    url = f"https://www.tweedekamer.nl/views/ajax?{urlencode(params)}"
    data = fetch_json(context, url, expected_charset=None)

    # This API returns a couple objects to update DOM state, out of which one
    # is a "insert" object containing the HTML we're interested in
    html = [
        obj["data"]
        for obj in data
        if "data" in obj and isinstance(obj["data"], str) and view_dom_id in obj["data"]
    ][0]

    doc = document_fromstring(html)
    for element in doc.findall('.//div[@class="m-card__content"]'):
        crawl_person(context, element, position)
