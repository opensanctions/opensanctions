from urllib.parse import urljoin, urlencode
from normality import stringify, collapse_spaces, slugify

from opensanctions.core import Context
from opensanctions import helpers as h
import re


def crawl_entity(context: Context, name: str, relative_url: str):
    url = urljoin(context.source.data.url, relative_url)
    doc = context.fetch_html(url)
    update_date_el = [el for el in doc.findall('.//span[@class="txt"]')
                      if "Profile updated" in el.text_content()][0]
    # Use name from index since "Profile updated" might be missing 
    # and is our strongest anchor point for the name container.
    # name = collapse_spaces(update_date_el.find("./../../div[3]/span").text_content())
    

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
            name = collapse_spaces(link.find('.//span[1]').text)

            type_span = link.find('.//span[2]')
            if type_span is None:
                context.log.info(f"No type classification for {name}")
                continue

            relative_url = link.get("href")

            if looks_like_name(name):
                crawl_entity(context, name, relative_url)
            else:
                # field arrangement might have changed.
                context.log.info(f"Not crawling {relative_url} because '{name}' doesn't look enough like a name")

        # break

        query["br"] = query["br"] + len(profiles)
