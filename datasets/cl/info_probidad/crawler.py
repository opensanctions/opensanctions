from pantomime.types import CSV
from urllib.parse import urlparse
from typing import Dict
import os
import csv

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise

def crawl_row(context: Context, row: Dict[str, str]):
    person = context.make("Person")

    id = os.path.basename(urlparse(row.pop("UriDeclarante")).path)
    person.id = context.make_slug(id)

    h.apply_name(
        person,
        first_name=row.pop("Nombre"),
        patronymic=row.pop("ApPaterno"),
        matronymic=row.pop("ApMaterno"),
    )

    position_name = row.pop("Cargo")
    position_institution = row.pop("Institucion")
    position = h.make_position(
        context, f"{position_name}, {position_institution}", country="cl", lang="spa"
    )
    
    # 'cargos' can be marked as PEPs for all institutions in cl_info_probidad.yml
    position_lookup = context.lookup("positions", position_name)
    if not position_lookup:
        context.log.warning(f"A new 'Cargo' '{position_name}' was identified")
    
    categorisation = categorise(context, position, is_pep = getattr(position_lookup, "is_pep", None))
    
    if not categorisation.is_pep:
        return
    
    context.emit(person, target=True)
    context.emit(position)

def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.dataset.data.url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
