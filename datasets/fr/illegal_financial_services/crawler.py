import csv
from typing import Dict
from rigour.mime.types import CSV
from normality import slugify

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):

    entity = context.make("LegalEntity")
    entity.id = context.make_id(row.get("nom"))
    entity.add("name", row.pop("nom"))
    entity.add("topics", "crime.fin")

    sanction = h.make_sanction(context, entity)
    sanction.add("date", row.pop("date_inscription"))
    sanction.add("summary", "Category: %s" % row.pop("categorie"))

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context):
    response = context.fetch_html(context.data_url)
    csv_url = response.find('.//*[@title="Télécharger le fichier"]').get("href")
    path = context.fetch_resource("source.csv", csv_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path, "r") as fh:
        for record in csv.DictReader(fh, delimiter=";"):
            row_ = {slugify(k, "_"): str(v) for k, v in record.items() if v is not None}
            row = {k: v for k, v in row_.items() if k is not None}
            crawl_item(row, context)
