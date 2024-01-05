from lxml import html, etree
from datetime import datetime
from zavod import Context
from zavod import helpers as h


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
    person_data["term_end"] = _extract_text(element, ".//td[6]/text()")
    person_data["block"] = _extract_text(element, ".//td[7]/text()")

    context.log.info(f"Finished crawl person", person_data)
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
    context.log.info("Starting crawling personal page", person_data["url"])
    response = context.http.get(person_data["url"])
    doc = html.fromstring(response.text)

    # Extract additional details from the personal page
    person_data["profession"] = _extract_text(
        doc, './/p[@class="encabezadoProfesion"]/span/text()'
    )
    birth_date = _extract_text(doc, './/p[@class="encabezadoFecha"]/span/text()')
    person_data["birth_date"] = convert_date_format(birth_date) if birth_date else None
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
            first_name=person_data["first_name"],
            last_name=person_data["last_name"],
        )
        person.add("sourceUrl", person_data["url"])
        person.add("political", person_data["block"])

        # Create position and occupancy
        position = h.make_position(
            context,
            name="Member of National Congress of Argentina",
            country="ar",
            subnational_area=person_data.get("district"),
        )
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            no_end_implies_current=True,
            start_date=person_data["term_start"],
            end_date=person_data["term_end"],
        )

        # Emit person, position and occupancy
        context.emit(person, target=True)
        if occupancy:
            context.log.info("Created Occupancy", occupancy)
            context.emit(occupancy)
        if position:
            context.log.info("Created Position", position)
            context.emit(position)


def convert_date_format(date_string: str) -> str:
    """
    Converts a date string from 'DD/MM/YYYY' format to 'YYYY-MM-DD'.
    """
    return datetime.strptime(date_string, "%d/%m/%Y").strftime("%Y-%m-%d")


def crawl(context: Context):
    """
    Main function to crawl and process data from the Argentinian parliament members page.
    """
    response = context.http.get(context.dataset.data.url)
    doc = html.fromstring(response.text)

    for element in doc.xpath("//tbody/tr"):
        person_data = crawl_person(context, element)
        make_and_emit_person(context, person_data)
