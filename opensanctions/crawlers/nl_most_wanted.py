from urllib.parse import urljoin
from lxml.etree import _Element
from normality import slugify

from zavod import Context
from opensanctions import helpers as h

base_url = "https://www.politie.nl/"

FORMATS = ("%d-%m-%Y",)

FIELDS = {
    "name": "name",
    "alias": "alias",
    "gender": "gender",
    "sex": "gender",
    "nationality": "nationality",
    "place_of_birth": "birthPlace",
    "other_physical_charateristics": None,
    "other_physical_characteristics": None,
    "length": None,
    "lenght": None,
    "height": None,
    "other": None,
    "tattoo": None,
    "build": None,
    "eye_colour": None,
    "eye_color": None,
    "eyes": None,
    "skin_colour": None,
    "hair_colour": None,
    "hair": None,
    "hair_color": None,
    "haircolor": None,
    "case": None,
    "police_region": None,
}


def crawl_person(context: Context, list_item: _Element):
    source_url = urljoin(context.data_url, list_item.get("href"))
    person = context.make("Person")
    person.id = context.make_id(source_url)
    person.add("topics", "crime")
    person.add("sourceUrl", source_url)

    doc = context.fetch_html(source_url)
    person.add("name", doc.findtext(".//h1"))

    description = doc.find('.//section[@aria-labelledby="omschrijving-title"]')
    descs = h.clean_note([p.text for p in description.findall("./p")])
    person.add("notes", "\n".join(descs))
    facts = {}
    for facts_el in doc.findall('.//dl[@id="gegevens-title-dl"]'):
        facts_key = None
        for el in facts_el.getchildren():
            if el.tag == "dt":
                facts_key = slugify(el.text, sep="_")
            if el.tag == "dd" and facts_key is not None:
                facts[facts_key] = el.text
                facts_key = None

    for field, value in facts.items():
        if field == "date_of_birth":
            date = h.parse_date(value.replace(" ", ""), FORMATS)
            person.add("birthDate", date)
            continue

        if field not in FIELDS:
            context.log.warn("Unkown descriptor", field=field, value=value)
            continue

        prop = FIELDS.get(field)
        if prop is not None:
            person.add(prop, value)

    context.emit(person, target=True)


def crawl(context: Context):
    page = 1
    while True:
        doc = context.fetch_html(context.data_url, params={"page": page})
        for item in doc.findall('.//section//a[@class="imagelistlink"]'):
            crawl_person(context, item)

        if doc.find('.//a[@rel="next"]') is None:
            break
        page += 1
    # crawl_pages(context, 1, False, "")
