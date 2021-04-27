from lxml import html
from pprint import pprint  # noqa

UI_URL = "https://www.cia.gov%s"
DATA_URL = "https://www.cia.gov/page-data%spage-data.json"


def crawl_country(context, params, path, country):
    source_url = UI_URL % path
    context.log.info("Crawling country: %s" % country)
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
        person = context.make("Person")
        person.make_slug(country, name, function)
        person.add("name", name)
        person.add("country", country)
        person.add("position", function)
        person.add("sourceUrl", source_url)
        person.add("topics", "role.pep")
        # pprint(person.to_dict())
        context.emit(person, target=True, unique=True)


def crawl(context):
    params = {"_": context.run_id}
    res = context.http.get(context.dataset.data.url, params=params)
    data = res.json().get("result", {}).get("data", {})
    for edge in data.get("governments", {}).get("edges", []):
        node = edge.get("node", {})
        crawl_country(context, params, node.get("path"), node.get("title"))
