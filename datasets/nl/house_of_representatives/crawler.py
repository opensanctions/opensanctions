from zavod import Context
from lxml.etree import _Element
from lxml.html import document_fromstring


def crawl_person(context: Context, element: _Element):
    anchor = element.find(".//a")
    person = context.make("Person")
    person.id = context.make_id(anchor.get("href"))

    person.add("name", anchor.text)
    person.add("topics", "role.pep")

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
