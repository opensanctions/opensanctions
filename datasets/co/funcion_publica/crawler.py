from normality import collapse_spaces
from pantomime.types import CSV
from typing import Dict
import csv

from zavod import helpers as h
from zavod.context import Context

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


def crawl_row(context: Context, row: Dict[str, str]):
    person = context.make("Person")
    id_number = row.pop("NUMERO_DOCUMENTO")
    person.id = context.make_slug(id_number)
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
        no_end_implies_current=True,
        start_date=row.pop("FECHA_VINCULACION"),
        end_date=row.pop("FECHA_DESVINCULACION"),
    )
    if occupancy:
        context.emit(person, target=True)
        context.emit(position)
        context.emit(occupancy)
    context.audit_data(row, ["ENLACE_CONSULTA_DECLARACIONES_PEP"])


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
