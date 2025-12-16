import json
from lxml import html
from rigour.mime.types import JSON, HTML

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise


HTML_DATA_URL = "https://www.hcdn.gob.ar/diputados/"


def crawl_json(context: Context) -> None:
    path = context.fetch_resource("source.json", context.data_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        data = json.load(fh)
    for entry in data:
        deputy_id = entry.pop("id")
        last_name = entry.pop("apellido")
        first_name = entry.pop("nombre")

        person = context.make("Person")
        person.id = context.make_id(first_name, last_name, deputy_id)
        h.apply_name(person, first_name=first_name, last_name=last_name)
        person.add("citizenship", "ar")
        person.add("gender", entry.pop("genero"))
        person.add("political", entry.pop("bloque"))

        position = h.make_position(
            context,
            name="Member of the Argentine Chamber of Deputies",
            wikidata_id="Q18229570",
            country="ar",
            topics=["gov.legislative", "gov.national"],
            lang="eng",
        )
        categorisation = categorise(context, position, is_pep=True)

        occupancy = h.make_occupancy(
            context,
            person,
            position,
            False,  # every tenure should have an end date (even if it is in the future)
            start_date=entry.pop("inicio"),
            end_date=entry.pop("fin"),
            categorisation=categorisation,
        )
        if occupancy is not None:
            context.emit(occupancy, external=True)
            context.emit(position, external=True)
            context.emit(person, external=True)

            context.audit_data(
                entry, ["distrito", "juramento", "cese", "bloque_inicio", "bloque_fin"]
            )


def _extract_text(element, xpath_query):
    result = element.xpath(xpath_query)
    return result[0].strip() if result else None


def crawl_personal_page(context: Context, url):
    context.log.debug("Starting crawling personal page", url=url)
    doc = context.fetch_html(url, cache_days=1)

    # Extract additional details from the personal page
    profession = _extract_text(doc, './/p[@class="encabezadoProfesion"]/span/text()')
    birth_date = _extract_text(doc, './/p[@class="encabezadoFecha"]/span/text()')
    email = _extract_text(doc, './/a[starts-with(@href, "mailto:")]/text()')

    return profession, birth_date, email


def crawl(context: Context):
    crawl_json(context)
    path = context.fetch_resource("source.html", HTML_DATA_URL)
    context.export_resource(path, HTML, title=context.SOURCE_TITLE)

    with open(path, "r") as fh:
        doc = html.parse(fh)
        doc = doc.getroot()
        doc.make_links_absolute(HTML_DATA_URL)

    # Find the table containing the deputy data
    for row in h.parse_html_table(doc.find(".//table")):
        str_row = h.cells_to_str(row)
        name_el = row.pop("diputado")
        link = name_el.xpath(".//a/@href")

        name = str_row.pop("diputado")
        if "pendiente de incorporacion" in name.lower():
            continue

        # Create and emit the person entity
        person = context.make("Person")
        person.id = context.make_id(name)
        first_name = h.multi_split(name, [", "])[1]
        last_name = h.multi_split(name, [", "])[0]
        h.apply_name(person, first_name=first_name, last_name=last_name)
        person.add("country", "ar")
        person.add("political", str_row.pop("bloque"))
        if link:
            profession, birth_date, email = crawl_personal_page(context, link[0])
            h.apply_date(person, "birthDate", birth_date)
            person.add("notes", profession)
            person.add("email", email)
            person.add("sourceUrl", link)
        else:
            context.log.warning("No link found for this person", name=name)
            return

        # Create position and categorize
        position = h.make_position(
            context, name="Member of National Congress of Argentina", country="ar"
        )
        categorisation = categorise(context, position, is_pep=True)

        # Create occupancy information
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            False,
            start_date=str_row.pop("inicia_mandato"),
            end_date=str_row.pop("finaliza_mandato"),
            categorisation=categorisation,
        )
        if occupancy:
            occupancy.add("description", str_row.pop("distrito"))

        # Emit the entities
        context.emit(person)
        context.emit(position)
        context.emit(occupancy)
        context.audit_data(str_row, ignore=["mandato"])
