from pantomime.types import CSV
from urllib.parse import urlparse
from typing import Dict
import os
import csv

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise

IGNORE_COLUMNS = [
    "UriDeclaracion",
    "Tipo",
    "ComunaDesempenio",
    "Comuna",
    "Direccion",
    "JefeServicio",
    "EstadoCivil",
    "Declaracion",
    "Grado",
    "PaisDesempenio",
    "Profesion",
    "RegimenPat",
]


def crawl_row(context: Context, row: Dict[str, str]):
    person = context.make("Person")

    person_uri = row.pop("UriDeclarante")
    id = os.path.basename(urlparse(person_uri).path)
    person.id = context.make_slug(id)

    h.apply_name(
        person,
        first_name=row.pop("Nombre"),
        patronymic=row.pop("ApPaterno"),
        matronymic=row.pop("ApMaterno"),
    )

    person.add("sourceUrl", person_uri)

    position_name = row.pop("Cargo")
    position_institution = row.pop("Institucion")
    position = h.make_position(
        context, f"{position_name}, {position_institution}", country="cl", lang="spa"
    )

    # 'cargos' can be marked as PEPs for all institutions in cl_info_probidad.yml
    res = context.lookup("positions", position_name)
    if not res:
        context.log.warning(f"A new 'Cargo' (post) '{position_name}' was identified")

    categorisation = categorise(context, position, res.is_pep)

    if not categorisation.is_pep:
        return

    start_date = row.pop("Asuncion")
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=False,
        start_date=start_date,
        categorisation=categorisation,
    )

    if occupancy:
        context.emit(person, target=True)
        context.emit(position)
        context.emit(occupancy)

    context.audit_data(row, IGNORE_COLUMNS)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.dataset.data.url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
