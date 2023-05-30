from urllib.parse import urljoin, urlencode
from normality import stringify, collapse_spaces, slugify

from opensanctions.core import Context
from opensanctions import helpers as h
import re

COUNTRY = "md"
PERSON_TYPES = {
    "police-officer",
    "russian-politician",
    "politician",
    "businessman",
    "judge",
    "csm-member",
    "prosecutor",
    "cna-officer",
    "csm-member",
    "sis-officer"
    "former-prosecutor",
}

def parse_date(text):
    return h.parse_date(text, ["%d.%m.%Y"])


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
            attributes[slugify(parts[0])] = collapse_spaces(parts[1])
    
    type_el = name_el.getnext().getnext()
    if hasattr(type_el, "text"):
        type_slug = slugify(type_el.text)
        type_str = collapse_spaces(type_el.text)
    else:
        type_slug = None
        type_str = None

    if type_slug in PERSON_TYPES:
        make_person(context, url, name, type_str, attributes)
    elif type_slug == "company":
        context.log.info(f"Skipping company {name}", url)
    else:
        context.log.info(f"Skipping unknown type {type_slug} for {name}", url)


def make_person(context: Context, url: str, name: str, position: str | None, attributes: dict) -> None:
    person = context.make("Person")
    identification = [COUNTRY, name]
    person.add("sourceUrl", url)
    person.add("name", name)
    if position:
        person.add("position", position)
    if "date-of-birth" in attributes:
        dob = parse_date(attributes.pop("date-of-birth"))
        identification.append(dob)
        person.add("birthDate", dob)
    if "place-of-birth" in attributes:
        person.add("birthPlace", attributes.pop("place-of-birth"))
    if "citizenship" in attributes:
        person.add("nationality", attributes.pop("citizenship"))
    if attributes:
        context.log.info(f"More info to be added to {name}", attributes, url)
    person.id = context.make_id(*identification)
    person.add("topics", "role.pep")
    context.emit(person, target=True)


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
        
        # check absurd offset just in case there are always results for some reason
        if not profiles or query["br"] > 10000:
            break

        for link in profiles:
            crawl_entity(context, link.get("href"))

        query["br"] = query["br"] + len(profiles)
