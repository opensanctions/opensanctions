from lxml import html

from opensanctions import settings
from opensanctions.core import Context
from opensanctions.util import is_empty

UI_URL = "https://www.cia.gov%s"
DATA_URL = "https://www.cia.gov/page-data%spage-data.json"


async def crawl_country(context: Context, params, path, country):
    source_url = UI_URL % path
    context.log.debug("Crawling country: %s" % country)
    res = context.http.get(DATA_URL % path, params=params)
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
        if is_empty(name):
            continue
        context.log.debug(
            "Person",
            country=country,
            name=name,
            function=function,
            url=source_url,
        )
        person = context.make("Person")
        person.id = context.make_slug(country, name, function)
        person.add("name", name)
        person.add("country", country)
        person.add("position", function)
        person.add("sourceUrl", source_url)
        person.add("topics", "role.pep")
        await context.emit(person, target=True)


async def crawl(context: Context):
    params = {"_": settings.RUN_DATE}
    res = context.http.get(context.dataset.data.url, params=params)
    data = res.json().get("result", {}).get("data", {})
    for edge in data.get("governments", {}).get("edges", []):
        node = edge.get("node", {})
        await crawl_country(context, params, node.get("path"), node.get("title"))
