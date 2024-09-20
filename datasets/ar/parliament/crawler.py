from rigour.mime.types import HTML
from lxml import html

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise

# CSV_LINK = "https://www.hcdn.gob.ar/system/modules/ar.gob.hcdn.diputados/formatters/generar-lista-diputados.csv"
BASE_URL = "https://www.hcdn.gob.ar"


def crawl_person(context: Context, row, element):
    first_name = row.pop("Nombre").text_content()
    last_name = row.pop("Apellido").text_content()
    # Create and emit the person entity
    person = context.make("Person")
    person.id = context.make_id(first_name, last_name)
    h.apply_name(person, first_name=first_name, last_name=last_name, lang="spa")
    person.add("country", "ar")
    person.add("political", row.pop("Bloque").text_content())
    person.add("notes", row.pop("Distrito").text_content())
    # Extract the relative link to the person's page (e.g., '/diputados/sacevedo/')
    relative_link = element.xpath(".//a/@href")[0]
    full_url = BASE_URL + relative_link
    # Get profession, birth_date, and email from personal page
    profession, birth_date, email = crawl_personal_page(context, full_url)
    # Call crawl_person with the current row data
    # h.apply_date(person, "birthDate", birth_date)
    person.add("birthDate", birth_date)
    person.add("notes", profession)
    person.add("email", email)

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
        start_date=h.parse_date(
            row.pop("IniciaMandato"), context.dataset.dates.formats
        )[0],
        end_date=h.parse_date(
            row.pop("FinalizaMandato"), context.dataset.dates.formats
        )[0],
        categorisation=categorisation,
    )

    if not occupancy:
        return
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

    table = doc.find(".//table")
    for row in h.parse_table(table):
        crawl_person(context, row)
