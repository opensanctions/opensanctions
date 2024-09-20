from rigour.mime.types import HTML
from lxml import html
from lxml.html import HtmlElement
from typing import Dict

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise

BASE_URL = "https://www.hcdn.gob.ar"


def crawl_person(context: Context, row: Dict[str, HtmlElement], element: HtmlElement):
    # str_row = h.cells_to_str(row)
    name = row.pop("diputado")
    mandate = row.pop("mandato")

    # Create and emit the person entity
    person = context.make("Person")
    person.id = context.make_id(name, mandate)
    person.add("name", name)
    person.add("country", "ar")
    person.add("political", row.pop("bloque"))
    person.add("notes", mandate)
    person.add("notes", row.pop("distrito"))

    # Extract the relative link to the person's page (e.g., '/diputados/sacevedo/')
    relative_link = element.xpath(".//a/@href")
    if relative_link:
        full_url = BASE_URL + relative_link[0]
        # Get profession, birth_date, and email from personal page
        profession, birth_date, email = crawl_personal_page(context, full_url)
        person.add("birthDate", birth_date)
        person.add("notes", profession)
        person.add("email", email)
    else:
        context.log.warning("No link found for this person", name=name)
        return

    # Create position and categorize
    position = h.make_position(
        context, name="Member of National Congress of Argentina", country="ar"
    )
    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        return

    # Create occupancy information
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        False,
        start_date=h.parse_date(
            row.pop("inicia_mandato"), context.dataset.dates.formats
        )[0],
        end_date=h.parse_date(
            row.pop("finaliza_mandato"), context.dataset.dates.formats
        )[0],
        categorisation=categorisation,
    )

    if not occupancy:
        return

    # Emit the entities
    context.emit(person, target=True)
    context.emit(position)
    context.emit(occupancy)
    context.audit_data(row)


def _extract_text(element, xpath_query):
    result = element.xpath(xpath_query)
    return result[0].strip() if result else None


def crawl_personal_page(context: Context, url):
    context.log.debug("Starting crawling personal page", url=url)
    doc = context.fetch_html(url, cache_days=1)

    # Extract additional details from the personal page
    profession = _extract_text(doc, './/p[@class="encabezadoProfesion"]/span/text()')
    birth_date = _extract_text(doc, './/p[@class="encabezadoFecha"]/span/text()')
    birth_date = h.parse_date(birth_date, context.dataset.dates.formats)
    email = _extract_text(doc, './/a[starts-with(@href, "mailto:")]/text()')

    return profession, birth_date, email


def crawl(context: Context):
    path = context.fetch_resource("source.html", context.data_url)
    context.export_resource(path, HTML, title=context.SOURCE_TITLE)

    with open(path, "r") as fh:
        doc = html.parse(fh)

    # Find the table containing the deputy data
    table = doc.find(".//table")

    for element in table.findall(".//tr"):
        # Skip header rows with <th> elements to parse only data rows
        if element.find(".//th") is not None:
            continue

        # Initialize the row dictionary that will hold the data for each deputy
        row = {}

        # Extract cells from the row and map them to columns
        cells = element.findall(".//td")

        if len(cells) >= 7:
            row["diputado"] = cells[1].text_content().strip()
            row["distrito"] = cells[2].text_content().strip()
            row["mandato"] = cells[3].text_content().strip()
            row["inicia_mandato"] = cells[4].text_content().strip()
            row["finaliza_mandato"] = cells[5].text_content().strip()
            row["bloque"] = cells[6].text_content().strip()
        else:
            context.log.warning(
                "Row does not have enough cells", row_html=html.tostring(element)
            )

        # Pass the row and row element to crawl_person
        crawl_person(context, row, element)
