from pantomime.types import CSV
from urllib.parse import urlparse
from typing import Dict
import os
import csv

from zavod import Context
from zavod import helpers as h

def crawl_row(context: Context, row: Dict[str, str]):
    entity = context.make("Person")
    
    id = os.path.basename(urlparse(row["UriDeclarante"]).path)
    entity.id = context.make_slug(id)
    h.apply_name(entity, first_name=row['Nombre'], patronymic=row['ApPaterno'], matronymic=row['ApMaterno'])
    context.emit(entity, target=True)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.dataset.data.url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
