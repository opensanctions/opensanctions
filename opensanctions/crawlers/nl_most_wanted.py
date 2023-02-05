from urllib.parse import urljoin
from lxml.etree import _Element
from normality import slugify
from dateutil.parser import ParserError, parse

from opensanctions.core import Context
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
    source_url = urljoin(context.source.data.url, list_item.get("href"))
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
    return

    doc = context.fetch_html(url)

    for x in doc.findall(".//section/ul/li"):

        name = x.find(".//h3").text_content().title()
        href = x.xpath(".//a")[0].get("href")

        person = context.make("Person")
        person.id = context.make_slug(name)
        person.add("topics", "crime")
        person.add("sourceUrl", (base_url + href))
        first_name, last_name = name.split(" ", 1)
        person.add("firstName", first_name)
        person.add("lastName", last_name)

        docPerson = context.fetch_html(base_url + href)
        i = 0

        for x in docPerson.findall('.//dl[@id="gegevens-title-dl"]/dt'):
            j = 0
            i = i + 1
            if x.text_content().title() == "Date Of Birth:":
                for y in docPerson.findall('.//dl[@id="gegevens-title-dl"]/dd'):
                    j = j + 1
                    if j == i:
                        try:
                            parsed_date = parse(y.text_content().title()).strftime(
                                "%d/%m/%Y"
                            )
                            person.add(
                                "birthDate",
                                h.parse_date(parsed_date, FORMATS),
                            )
                        except ParserError:
                            pass

            if x.text_content().title() == "Nationality:":
                for y in docPerson.findall('.//dl[@id="gegevens-title-dl"]/dd'):
                    j = j + 1
                    if j == i:
                        person.add("nationality", y.text_content().title())

            if x.text_content().title() == "Sex:":
                for y in docPerson.findall('.//dl[@id="gegevens-title-dl"]/dd'):
                    j = j + 1
                    if j == i:
                        person.add("gender", y.text_content().title())

        if first_person != name and not end_of_page:
            context.emit(person, target=True)
        else:
            end_of_page = True

        if first_person == "":
            first_person = name

    crawl_pages(context, index_crawl_pages, end_of_page, first_person)


def crawl(context: Context):
    page = 1
    while True:
        doc = context.fetch_html(context.source.data.url, params={"page": page})
        for item in doc.findall('.//section//a[@class="imagelistlink"]'):
            crawl_person(context, item)

        if doc.find('.//a[@rel="next"]') is None:
            break
        page += 1
    # crawl_pages(context, 1, False, "")
