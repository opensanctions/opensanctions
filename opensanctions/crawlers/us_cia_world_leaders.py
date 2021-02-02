from urllib.parse import urljoin
from lxml import html
import requests
from pprint import pprint  # noqa
from datetime import datetime
from normality import collapse_spaces, stringify
from ftmstore.memorious import EntityEmitter

UI_URL = "https://www.cia.gov%s"
INDEX = "/resources/world-leaders/foreign-governments/"
DATA_URL = "https://www.cia.gov/page-data%spage-data.json"


def element_text(el):
    if el is None:
        return
    text = stringify(el.text_content())
    if text is not None:
        return collapse_spaces(text)


def parse_updated(text):
    text = text.strip()
    try:
        return datetime.strptime(text, "%d %b %Y").date()
    except ValueError:
        pass


def crawl_country(context, path, country):
    emitter = EntityEmitter(context)
    source_url = UI_URL % path
    context.log.info("Crawling country: %s", country)

    res = requests.get(DATA_URL % path)
    data = res.json().get("result", {}).get("data", {}).get("page", {})
    blocks = data.get("acf", {}).get("blocks", [{}])[0]
    content = blocks.get("free_form_content", []).get("content")
    doc = html.fromstring(content)
    function = None
    for i, el in enumerate(doc.getchildren()):
        text = el.text_content().strip()
        if el.tag == "h2":
            continue
        if el.tag == "h3":
            function = text
            continue
        if i == 0 and el.tag == "p":
            # this paragraph at the start is a note, not a person
            continue
        name = text.replace("(Acting)", "")
        person = emitter.make("Person")
        person.make_id(source_url, name, function)
        person.add("name", name)
        person.add("country", country)
        person.add("position", function)
        person.add("sourceUrl", source_url)
        person.add("topics", "role.pep")
        # pprint(person.to_dict())
        emitter.emit(person)
    emitter.finalize()


def crawl(context, data):
    res = requests.get(DATA_URL % INDEX)
    data = res.json().get("result", {}).get("data", {})
    for edge in data.get("governments", {}).get("edges", []):
        node = edge.get("node", {})
        crawl_country(context, node.get("path"), node.get("title"))
