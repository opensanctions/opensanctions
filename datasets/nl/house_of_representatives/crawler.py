from zavod import Context
from urllib.parse import urljoin
from lxml.etree import _Element
from lxml.html import document_fromstring
import re


def crawl_person(context: Context, element: _Element):
    anchor = element.find(".//a")
    source_url = urljoin(context.data_url, anchor.get("href"))

    person = context.make("Person")
    person.id = context.make_id(source_url)

    person.add("topics", "role.pep")
    person.add("sourceUrl", source_url)

    doc = context.fetch_html(source_url)
    section = doc.find('.//main[@class="o-main"]/article/section[2]')

    person.add("name", section.findtext(".//h1"))

    match = re.search(
        "is geboren in (.+) op (.+) en woont in", section.findtext(".//p")
    )
    person.add("birthPlace", match.group(1))
    # TODO - Normalize, this is a dutch written out date
    person.add("birthDate", match.group(2))

    context.emit(person, target=True)


def crawl(context: Context):
    view_dom_id = "0183a2676e03bf17f9a9e6e5c225aafc3743864c3f4f2795cb8710f07c50deec"

    params = {
        "view_name": "members_of_parliament",
        "view_display_id": "page_all_members",
        "view_dom_id": view_dom_id,
    }

    data = context.fetch_json("https://www.tweedekamer.nl/views/ajax", params=params)

    # This API returns a couple objects to update DOM state, out of which one
    # is a "insert" object containing the HTML we're interested in
    html = [
        obj["data"]
        for obj in data
        if "data" in obj and isinstance(obj["data"], str) and view_dom_id in obj["data"]
    ][0]

    doc = document_fromstring(html)
    for element in doc.findall('.//div[@class="m-card__content"]'):
        crawl_person(context, element)
