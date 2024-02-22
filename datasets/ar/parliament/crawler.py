from lxml import html, etree
from datetime import datetime
from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise

FORMATS = ["%d/%m/%Y"]


def crawl_person(context: Context, element) -> dict:
    """
    Extracts information about a person from the Parliament page.
    Returns a dictionary containing extracted data about the person.
    """
    person_data = {}

    # Extract PEP's image URL
    image_url = element.xpath(".//td[1]/img/@src")
    person_data["image_url"] = image_url[0].strip() if image_url else None

    # Extract PEP's name, alias and split name into first and last names
    name = element.xpath(".//td[2]/a/text()")
    if name:
        name_parts = name[0].strip().split(",")
        person_data.update(
            {
                "name": name[0].strip(),
                "alias": f"{name_parts[1]} {name_parts[0]}",
                "first_name": name_parts[1],
                "last_name": name_parts[0],
            }
        )

    # Extract URL extension for the personal page and save person url if exists
    url_extension = element.xpath(".//td[2]/a/@href")
    if url_extension:
        person_data["url"] = context.dataset.data.url + url_extension[
            0
        ].strip().replace("/diputados/", "")
        crawl_personal_page(context, person_data)
    else:
        person_data.update(
            {
                "url": context.dataset.data.url,
                "profession": None,
                "birth_date": None,
                "email": None,
            }
        )

    # Extract additional available information about the person
    person_data["district"] = _extract_text(element, ".//td[3]/text()")
    person_data["term"] = _extract_text(element, ".//td[4]/text()")
    person_data["term_start"] = _extract_text(element, ".//td[5]/text()")
    if person_data["term_start"]:
        person_data["term_start"] = h.parse_date(person_data["term_start"], FORMATS)[0]
    person_data["term_end"] = _extract_text(element, ".//td[6]/text()")
    if person_data["term_end"]:
        person_data["term_end"] = h.parse_date(person_data["term_end"], FORMATS)[0]
    person_data["block"] = _extract_text(element, ".//td[7]/text()")

    context.log.debug(f"Finished crawl person", person=person_data)
    return person_data


def _extract_text(element, xpath_query):
    """
    Helper function to extract text content from an HTML element using XPath.
    Returns the extracted text, or None if not found.
    """
    result = element.xpath(xpath_query)
    return result[0].strip() if result else None


def crawl_personal_page(context: Context, person_data: dict):
    """
    Crawls a person's individual page and updates their data dictionary.
    """
    context.log.debug("Starting crawling personal page", person=person_data["url"])
    doc = context.fetch_html(person_data["url"], cache_days=1)

    # Extract additional details from the personal page
    person_data["profession"] = _extract_text(
        doc, './/p[@class="encabezadoProfesion"]/span/text()'
    )
    birth_date = _extract_text(doc, './/p[@class="encabezadoFecha"]/span/text()')
    person_data["birth_date"] = h.parse_date(birth_date, FORMATS)
    person_data["email"] = _extract_text(
        doc, './/a[starts-with(@href, "mailto:")]/text()'
    )


def make_and_emit_person(context: Context, person_data: dict):
    """
    Creates a 'Person' entity from the scraped data and emits it to the Context.
    """
    if person_data.get("name"):
        person = context.make("Person")
        person.id = context.make_id(person_data["name"])
        person.add("name", person_data["name"])
        person.add("alias", person_data["alias"])
        person.add("country", "ar")
        h.apply_name(
            person,
            first_name=person_data["first_name"].strip(),
            last_name=person_data["last_name"].strip(),
        )
        person.add("sourceUrl", person_data["url"])
        person.add("political", person_data["block"])

        position = h.make_position(
            context,
            name="Member of National Congress of Argentina",
            country="ar",
        )
        categorisation = categorise(context, position, is_pep=True)
        if not categorisation.is_pep:
            return
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            False,
            start_date=person_data["term_start"],
            end_date=person_data["term_end"],
            categorisation=categorisation,
        )

        if not occupancy:
            return
        context.emit(person, target=True)
        context.emit(position)
        context.emit(occupancy)


def crawl(context: Context):
    """
    Main function to crawl and process data from the Argentinian parliament members page.
    """
    doc = context.fetch_html(context.dataset.data.url, cache_days=1)

    for element in doc.xpath("//tbody/tr"):
        person_data = crawl_person(context, element)
        make_and_emit_person(context, person_data)
