from datetime import datetime
from normality import collapse_spaces, slugify
from pantomime.types import CSV
from typing import Dict, List, Tuple
import csv

from zavod import helpers as h
from zavod.context import Context
from zavod.logic.pep import OccupancyStatus, backdate

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
    position = h.make_position(context, position_name, country="co", topics=topics)
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        # no_end_implies_current=True,
        # Data Dictionary says "The box will be empty if the entity has not 
        # reported the separation of the Politically Exposed Person."
        # but that's not the case. Have an issue open with their support.
        # In the meantime:
        status=OccupancyStatus.UNKNOWN,
        start_date=row.pop("FECHA_VINCULACION"),
        end_date=row.pop("FECHA_DESVINCULACION"),
    )
    context.emit(person, target=True)
    context.emit(position)
    context.emit(occupancy)
    context.audit_data(row, ["ENLACE_CONSULTA_DECLARACIONES_PEP"])
    return slugify([id_number, role, entity_name])


def crawl_table_row(context: Context, seen: set, row: Dict[str, str|List[Tuple[str, str]]]):
    name_id = row.pop("declarante").split(" - ")
    if len(name_id) != 2:
        context.log.warning("Invalid name/id", name_id=name_id)
        return
    role = row.pop("cargo")
    entity_name = row.pop("entidad")
    key = slugify([name_id[1], role, entity_name])
    if key in seen:
        return
    
    if row.pop("fecha-publicacion") < backdate(datetime.now(), 365*5):
        context.log.warning("Skiping potentially too old position", key=key)
        return
    
    if row.pop("es-contratista") != "NO":
        context.log.warning("Unexpectedly found a contractor", key=key)
        return
    
    person = context.make("Person")
    person.id = context.make_slug(name_id[1], prefix="co-cedula")
    person.name = name_id[0]
    person.add("idNumber", name_id[1])
    links = row.pop("enlaces-externos")
    person.add("website", links.pop("consultar-hoja-de-vida", None))
    person.add(
        "notes",
        (
            "Find their declarations of assets and income, conflicts of interest"
            " and income and complementary taxes (Law 2013 of 2019) at "
            f'{links.pop("consultar-declaraciones-ley-2013-de-2019")}'
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
    position = h.make_position(context, position_name, country="co", topics=topics)
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=False,
        status=OccupancyStatus.UNKNOWN,
    )
    context.emit(person, target=True)
    context.emit(position)
    context.emit(occupancy)
    context.audit_data(row, ["descargar"])


def parse_table(table) -> List[Dict[str, str|Dict[str, str]]]:
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = []
            for el in row.findall("./th"):
                headers.append(slugify(el.text_content()))
            continue
        cells = []
        for el in row.findall("./td"):
            anchors = el.findall("./a")
            links = {}
            for anchor in anchors:
                links[slugify(anchor.text_content())] = anchor.get("href")
            if links:
                cells.append(links)
            else:
                cells.append(collapse_spaces(el.text_content()))
        assert len(headers) == len(cells), (headers, cells)
        yield {hdr: c for hdr, c in zip(headers, cells)}


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
        doc = context.fetch_html(next_link, cache_days=1)
        doc.make_links_absolute(next_link)
        next_anchors = doc.xpath("//a[contains(@class, 'nextLink')]")
        if next_anchors:
            next_link = next_anchors[0].get("href")
        else:
            next_link = None

        for row in parse_table(doc.find(".//table")):
            crawl_table_row(context, seen, row)
            