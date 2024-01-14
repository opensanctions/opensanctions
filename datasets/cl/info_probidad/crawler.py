from pantomime.types import CSV
from urllib.parse import urlparse
from typing import Dict
import os
import csv

from zavod import Context
from zavod import helpers as h

def crawl_row(context: Context, row: Dict[str, str]):
    entity = context.make("Person")
    
    uri = row.pop("UriDeclarante")
    id = os.path.basename(urlparse(uri).path)
    entity.id = context.make_slug(id)
    name = row.pop("Nombre")
    father_name = row.pop("ApPaterno")
    mother_name = row.pop("ApMaterno")
    h.apply_name(entity, first_name=name, patronymic=father_name, matronymic=mother_name)
    context.emit(entity, target=True)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.dataset.data.url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
