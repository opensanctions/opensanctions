from urllib.parse import urljoin, urlencode
from normality import stringify, collapse_spaces, slugify

from opensanctions.core import Context
from opensanctions import helpers as h
import re


def crawl_entity(context: Context, relative_url: str):
    url = urljoin(context.source.data.url, relative_url)
    doc = context.fetch_html(url)
    name_el = doc.find('.//span[@class="name"]')
    name = collapse_spaces(name_el.text)
    attributes = dict()
    for el in name_el.find("./..").getnext().getchildren():
        text = collapse_spaces(el.text_content())
        parts = text.split(": ")
        if len(parts) == 2:
            attributes[parts[0]] = parts[1]
    print(name, attributes)
    

def looks_like_name(text):
    """True if the string is two or more title case words."""
    return bool(re.match("([A-Z]\w+ ?){2,}", text))
    
def crawl_person(context: Context, url):
    context.log.debug("Crawling person", url=url)
    person = context.make("Person")
    person.id = context.make_slug(person_id)
    person.add("sourceUrl", url)
    person.add("name", name)
    person.add("topics", "role.pep")


def crawl(context: Context):
    query = {
        "br": 0,
        "lang": "eng"
    }
    while True:

        context.log.debug("Crawling index offset ", query)
        url = f"{ context.source.data.url }?{ urlencode(query) }"
        doc = context.fetch_html(url)
        profiles = doc.findall('.//div[@class="profileWindow"]//a')
        
        if not profiles:
            break

        for link in profiles:
            crawl_entity(context, link.get("href"))

        query["br"] = query["br"] + len(profiles)
