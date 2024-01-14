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
    position = h.make_position(
        context, position_name, country="cl", lang="spa"
    )
    position_lookup = context.lookup("positions", position_name)
    
    # all positions (ie. Cargo) should be explicitly classified as either pep or no-pep
    if not position_lookup:
        context.audit_data(position.to_dict())
    
    if not position_lookup.is_pep:
        return

    categorisation = categorise(context, position)
    
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
