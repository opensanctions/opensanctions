from urllib.parse import urljoin, urlencode
from normality import stringify, collapse_spaces, slugify

from opensanctions.core import Context
from opensanctions import helpers as h
import re

CACHE_DAYS=14
COUNTRY = "md"
PERSON_TYPES = {
    "politiciana",
    "politist",
    "politician",
    "politician-rus",
    "guvernator-bnm",
    "sef-de-directie-bnm",
    "businessman",
    "judecatoare",
    "judecator",
    "membra-csm",
    "procuror",
    "ex-viceprim-ministra",
    "ofitera-cna",
    "viceguvernator-bnm",
    "prim-ministru-interimar",
    "membru-csm",
    "ex-procuror",
    "director-cnam",
    "consiliera-prezidentiala",
    "fost-guvernator-bnm",
    "femeie-de-afaceri",
    "membru-al-consiliului-de-supraveghere-al-bnm",
    "fost-director-cna",
    "ex-bascan",
    "consilier-al-guvernatorului-bnm",
    "prim-ministra",
    "ofiter-sis",
    "membru-al-consiliului-de-supraveghere-a-bnm",
    "executoare-judecatoreasca",
    "procurora",
    "prim-viceguvernator-bnm",
    "fost-judecator",
    "director",
    "consilier-prezidential",
    "ministra",
    "functionara",
    "ministru",
    "avocat",
    "diplomat",
    "sef-directie-bnm",
    "presedinta-republicii-moldova",
    "directoare-inj",
    "deputata",
    "investigator",
}

KNOWN_PERSONS = {
    "Vadim Ceban",
    "Galina Dodon",
    "Veronica Dragalin",
}

KNOWN_COMPANIES = {
    "Intertelecom",
    "Daniel-Marius Staicu",
    "Dinu Țurcanu",
}


def parse_date(text):
    return h.parse_date(text, ["%d.%m.%Y"])


def crawl_entity(context: Context, relative_url: str):
    url = urljoin(context.source.data.url, relative_url)
    doc = context.fetch_html(url, cache_days=CACHE_DAYS)
    name_el = doc.find('.//span[@class="name"]')
    name = collapse_spaces(name_el.text)
    attributes = dict()
    for el in name_el.find("./..").getnext().getchildren():
        text = collapse_spaces(el.text_content())
        parts = text.split(": ")
        if len(parts) == 2:
            attributes[slugify(parts[0])] = collapse_spaces(parts[1])

    if "Conexiuni" in doc.text_content():
        context.log.warn(f"There are connections to be added for {url}")

    type_el = name_el.getnext().getnext()
    if hasattr(type_el, "text"):
        type_slug = slugify(type_el.text)
        type_str = collapse_spaces(type_el.text)
    else:
        type_slug = None
        type_str = None

    if type_slug in PERSON_TYPES:
        make_person(context, url, name, type_str, attributes)
    elif name in KNOWN_PERSONS:
        make_person(context, url, name, None, attributes)
    elif type_slug == "companie" or name in KNOWN_COMPANIES:
        make_company(context, url, name, attributes)
    else:
        context.log.warn(f"Skipping unknown type {type_slug} for {name}", url)


def make_person(
    context: Context, url: str, name: str, position: str | None, attributes: dict
) -> None:
    person = context.make("Person")
    identification = [COUNTRY, name]
    person.add("sourceUrl", url)
    person.add("name", name)
    person.add("position", position, lang="ron")
    if "data-nasterii" in attributes:
        dob = parse_date(attributes.pop("data-nasterii"))
        identification.append(dob)
        person.add("birthDate", dob)

    person.add("birthPlace", attributes.pop("locul-nasterii", None), lang="ron")
    person.add("nationality", attributes.pop("cetatenie", "").split(","))

    if attributes:
        context.log.info(f"More info to be added to {name}", attributes, url)
    person.id = context.make_id(*identification)
    person.add("topics", "poi")
    context.emit(person, target=True)


def make_company(
    context: Context, url: str, name: str, attributes: dict
) -> None:
    company = context.make("Company")
    identification = [COUNTRY, name]
    company.add("sourceUrl", url)
    company.add("name", name)
    if "data-inregistrarii" in attributes:
        founded = parse_date(attributes.pop("data-inregistrarii"))
        identification.append(founded)
        company.add("incorporationDate", founded)

    country = attributes.pop("tara", "").split(",")[0]
    company.add("mainCountry", country)

    if "numar-de-identificare" in attributes:
        regno = attributes.pop("numar-de-identificare")
        identification.append(regno)
        company.add("registrationNumber", regno)
    if attributes:
        context.log.info(f"More info to be added to {name}", attributes, url)
    company.id = context.make_id(*identification)
    context.emit(company)


def crawl(context: Context):
    query = {"br": 0, "lang": "rom"}
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