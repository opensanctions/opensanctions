from typing import Dict, Any, Optional
from normality import slugify, collapse_spaces

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise

WEB_URL = "https://www.cia.gov/resources/world-leaders/foreign-governments/%s"
DATA_URL = "https://www.cia.gov/resources/world-leaders/page-data/foreign-governments/%s/page-data.json"
SECTIONS = ["leaders", "leaders_2", "leaders_3", "leaders_4"]


def clean_position(position: str):
    replacements = [
        ("Dep.", "Deputy"),
        ("Min.", "Minister"),
        ("Pres.", "President"),
        ("Gen.", "General"),
        ("Govt.", "Government"),
        ("Sec.", "Secretary"),
        ("Dir.", "Director"),
        ("Chmn.", "Chairman"),
        ("Intl.", "International"),
        ("Mbr.", "Member"),
    ]
    for abbrev, full in replacements:
        position = position.replace(abbrev, full)
    return position


def crawl_leader(
    context: Context,
    country: str,
    source_url: str,
    section: Optional[str],
    leader: Dict[str, Any],
) -> None:
    name = leader["name"]
    name = name.replace("(Acting)", "")
    if h.is_empty(name) or name.lower() == "vacant":
        return
    function = clean_position(leader["title"])
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
    person.add("position", function)
    person.add("sourceUrl", source_url)

    res = context.lookup("position_topics", function)
    if res:
        position_topics = res.topics
    else:
        position_topics = []
        context.log.info(
            "No topics match for position", position=function, country=country
        )

    position = h.make_position(
        context,
        function,
        country=country,
        topics=position_topics,
        id_hash_prefix="us_cia_world_leaders",
    )
    categorisation = categorise(context, position, True)
    if categorisation.is_pep:
        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )

        context.emit(person, target=True)
        context.emit(position)
        context.emit(occupancy)


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
