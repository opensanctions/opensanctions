from normality import slugify

from opensanctions.core import Context
from opensanctions import helpers as h

WEB_URL = "https://www.cia.gov/resources/world-leaders/foreign-governments/%s"
DATA_URL = "https://www.cia.gov/resources/world-leaders/page-data/foreign-governments/%s/page-data.json"


def crawl_country(context: Context, country: str):
    context.log.debug("Crawling country: %s" % country)
    country_slug = slugify(country.replace("'", ""), sep="-")
    data_url = DATA_URL % country_slug
    source_url = WEB_URL % country_slug
    res = context.fetch_json(data_url)
    page_data = res["result"]["data"]["page"]
    leaders = page_data["leaders"]
    for leader in leaders:
        name = leader["name"]
        name = name.replace("(Acting)", "")
        function = leader["title"]
        if h.is_empty(name):
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
        person.add("country", page_data["country"])
        person.add("position", function)
        person.add("sourceUrl", source_url)
        person.add("topics", "role.pep")
        context.emit(person, target=True)


def crawl(context: Context):
    data = context.fetch_json(context.source.data.url)
    countries = data["data"]["leaders"]["nodes"]
    for c in countries:
        crawl_country(context, c["country"])
