from typing import Dict, Any, Optional
from normality import slugify, collapse_spaces

from zavod import Context
from opensanctions import helpers as h

WEB_URL = "https://www.cia.gov/resources/world-leaders/foreign-governments/%s"
DATA_URL = "https://www.cia.gov/resources/world-leaders/page-data/foreign-governments/%s/page-data.json"
SECTIONS = ["leaders", "leaders_2", "leaders_3", "leaders_4"]


def crawl_leader(
    context: Context,
    country: str,
    source_url: str,
    section: Optional[str],
    leader: Dict[str, Any],
) -> None:
    name = leader["name"]
    name = name.replace("(Acting)", "")
    if h.is_empty(name):
        return
    function = leader["title"]
    gov = collapse_spaces(section)
    if gov:
        function = f"{function} - {gov}"
        # print(function)
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
    context.emit(person, target=True)


def crawl_country(context: Context, country: str) -> None:
    context.log.debug("Crawling country: %s" % country)
    country_slug = slugify(country.replace("'", ""), sep="-")
    data_url = DATA_URL % country_slug
    source_url = WEB_URL % country_slug
    res = context.fetch_json(data_url)
    page_data = res["result"]["data"]["page"]
    page_data.pop("code", None)
    page_data.pop("caveat", None)
    page_data.pop("country", None)
    page_data.pop("date_updated", None)
    page_data.pop("diplomatic_exchange", None)

    for section in SECTIONS:
        label = page_data.pop(f"{section}_label", "")
        for leader in page_data.pop(section, []):
            crawl_leader(context, country, source_url, label, leader)

    if len(page_data):
        context.log.warn("Extra data found in page_data", data=page_data)


def crawl(context: Context) -> None:
    data = context.fetch_json(context.data_url)
    countries = data["data"]["leaders"]["nodes"]
    for c in countries:
        crawl_country(context, c["country"])
