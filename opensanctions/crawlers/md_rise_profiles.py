from urllib.parse import urljoin, urlencode
from normality import stringify, collapse_spaces, slugify

from opensanctions.core import Context
from opensanctions import helpers as h


def crawl_person(context: Context, url):
    context.log.debug("Crawling person", url=url)
    person = context.make("Person")
    person.id = context.make_slug(person_id)
    person.add("sourceUrl", url)
    person.add("name", name)
    person.add("topics", "role.pep")

def crawl(context: Context):
    query = {"br": 0}
    while True:

        context.log.debug("Crawling index offset ", query)
        url = f"{ context.source.data.url }?{ urlencode(query) }"
        doc = context.fetch_html(url)
        profiles = doc.findall('.//div[@class="profileWindow"]//a')
        
        if not profiles:
            break

        for link in profiles:
            print(collapse_spaces(link.text_content()), link.get("href"))
            # crawl_person(context, url)

        query = {"br": query["br"] + len(profiles)}
