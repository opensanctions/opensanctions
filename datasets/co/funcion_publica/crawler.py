from datetime import datetime
from normality import slugify
from rigour.mime.types import CSV
from typing import Dict
import csv
from lxml.html import HtmlElement
import re

from zavod import helpers as h
from zavod.context import Context
from zavod.logic.pep import OccupancyStatus, backdate, categorise

# NUMERO DOCUMENTO
# NOMBRE PEP
# DENOMINACION CARGO
# NOMBRE ENTIDAD
# FECHA VINCULACION
# FECHA DESVINCULACION
# ENLACE CONSULTA DECLARACIONES PEP
# ENLACE HOJA VIDA SIGEP
# ENLACE CONSULTA LEY 2013 2019
#
# DOCUMENT NUMBER
# NAME PEP
# POSITION NAME
# ENTITY NAME
# LINK DATE
# DISASSEMBLY DATE
# LINK TO CONSULT PEP DECLARATIONS
# SIGEP LIFE SHEET LINK
# LINK CONSULTATION LAW 2013 2019

# Todos
# solo servidores publicos
# solo contratistas
#
# All
# only public servers
# contractors only

REGEX_ID = re.compile(
    r"""
    ^
    (?P<name>[\w‘“ ]+)
    (
        \ PASAPORTE\ -\ (?P<passport>\d+)|
        \ CEDULA\ DE\ CIUDADANIA\ -\ (?P<cedula>\d+)|
        \ CEDULA\ DE\ EXTRANJERIA\ -\ (?P<foreign>\d+)
    )
    $
    """,
    re.VERBOSE,
)


def crawl_sheet_row(context: Context, row: Dict[str, str]):
    person = context.make("Person")
    id_number = row.pop("NUMERO_DOCUMENTO")
    person.id = context.make_slug(id_number, prefix="co-cedula")
    person.add("idNumber", id_number)
    person.add("name", row.pop("NOMBRE_PEP"))
    cv_url = row.pop("ENLACE_HOJA_VIDA_SIGEP")
    if "https://www.funcionpublica.gov.co/web/sigep" in cv_url:
        if "hoja-de-vida-no-encontrada" not in cv_url:
            person.add("website", cv_url)
    else:
        context.log.warning("unknown cv url", url=cv_url)
    person.add(
        "notes",
        (
            "Find their declarations of assets and income, conflicts of interest"
            " and income and complementary taxes (Law 2013 of 2019) at "
            f'{row.pop("ENLACE_CONSULTA_LEY_2013_2019")}'
        ),
    )

    role = row.pop("DENOMINACION_CARGO")
    entity_name = row.pop("NOMBRE_ENTIDAD")
    res = context.lookup("positions", role)
    add_entity = True
    topics = None
    if res:
        add_entity = res.add_entity
        topics = res.topics
    position_name = role
    if add_entity:
        position_name += " - " + entity_name
    position = h.make_position(
        context, position_name, country="co", topics=topics, lang="spa"
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        # no_end_implies_current=True,
        # Data Dictionary says "The box will be empty if the entity has not
        # reported the separation of the Politically Exposed Person."
        # but that's not the case. I have an issue open with their support.
        # In the meantime:
        status=OccupancyStatus.UNKNOWN,
        start_date=row.pop("FECHA_VINCULACION"),
        end_date=row.pop("FECHA_DESVINCULACION"),
        categorisation=categorisation,
    )
    context.emit(person)
    context.emit(position)
    context.emit(occupancy)
    context.audit_data(row, ["ENLACE_CONSULTA_DECLARACIONES_PEP"])
    return slugify([id_number, role, entity_name])


def crawl_table_row(
    context: Context,
    seen: set,
    row: Dict[str, HtmlElement],
):
    str_row = h.cells_to_str(row)
    name_id = str_row.pop("declarante")
    person = context.make("Person")
    match = REGEX_ID.search(name_id)
    if match is None:
        context.log.warning("Invalid name/id", name_id=name_id)
        return
    if match.group("passport"):
        id_number = match.group("passport")
        person.id = context.make_slug(id_number, prefix="co-passport")
        person.add("passportNumber", id_number)
    if match.group("foreign"):
        id_number = match.group("foreign")
        person.id = context.make_slug(id_number, prefix="co-foreign")
        person.add("idNumber", id_number)
    if match.group("cedula"):
        id_number = match.group("cedula")
        person.id = context.make_slug(id_number, prefix="co-cedula")
        person.add("idNumber", id_number)

    person.add("name", match.group("name"))

    role = str_row.pop("cargo")
    entity_name = str_row.pop("entidad")
    key = slugify([id_number, role, entity_name])

    if key in seen:
        return

    if str_row.pop("fecha_publicacion") < backdate(datetime.now(), 365 * 5):
        context.log.warning("Skipping potentially too old position", key=key)
        return

    if str_row.pop("es_contratista") != "NO":
        context.log.warning("Unexpectedly found a contractor", key=key)
        return

    links = h.links_to_dict(row.pop("enlaces_externos"))

    person.add("website", links.pop("consultar_hoja_de_vida", None))
    person.add(
        "notes",
        (
            "Find their declarations of assets and income, conflicts of interest"
            " and income and complementary taxes (Law 2013 of 2019) at "
            f'{links.pop("consultar_declaraciones_ley_2013_de_2019")}'
        ),
    )

    res = context.lookup("positions", role)
    add_entity = True
    topics = None
    if res:
        add_entity = res.add_entity
        topics = res.topics
    position_name = role
    if add_entity:
        position_name += " - " + entity_name

    position = h.make_position(
        context, position_name, country="co", topics=topics, lang="spa"
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=False,
        status=OccupancyStatus.UNKNOWN,
        categorisation=categorisation,
    )
    context.emit(person)
    context.emit(position)
    context.emit(occupancy)
    context.audit_data(str_row, ["descargar", "enlaces_externos"])


def crawl(context: Context):
    seen = set()
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            seen.add(crawl_sheet_row(context, row))

    next_link = "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/consultaPEP?find=FindNext&tipoRegistro=4&offset=0&max=50"
    while next_link:
        context.log.info("Fetching page", url=next_link)
        doc = context.fetch_html(next_link)
        doc.make_links_absolute(next_link)
        step_anchors = doc.xpath("//a[contains(@class, 'step')]")
        context.log.info("Pages", pagenums=[a.text_content() for a in step_anchors])
        if not step_anchors:
            context.log.warning("No pagination found", url=next_link)
        next_anchors = doc.xpath("//a[contains(@class, 'nextLink')]")
        if next_anchors:
            next_link = next_anchors[0].get("href")
        else:
            next_link = None

        for row in h.parse_html_table(doc.find(".//table")):
            crawl_table_row(context, seen, row)
