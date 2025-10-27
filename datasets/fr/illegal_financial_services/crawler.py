import csv
from typing import Dict
from rigour.mime.types import CSV
from normality import slugify

from zavod import Context, helpers as h
from followthemoney.types import registry


def crawl_item(row: Dict[str, str], context: Context):
    name = row.pop("nom")
    entity = context.make("LegalEntity")
    entity.id = context.make_id(name)
    entity.add("name", name)
    entity.add("topics", "crime.fin")
    # name is the website address, (generalized) email
    # address or name of the malicious actor
    email_clean = registry.email.clean(name)
    if email_clean is not None:
        entity.add("email", email_clean)
    url_clean = registry.url.clean(name)
    if url_clean is not None:
        entity.add("website", url_clean)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "date", row.pop("date_inscription"))
    entity.add("sector", row.pop("categorie"), lang="fra")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context):
    response = context.fetch_html(context.data_url)
    csv_url = response.find('.//*[@title="Télécharger le fichier en csv"]').get("href")
    path = context.fetch_resource("source.csv", csv_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path, "r") as fh:
        for record in csv.DictReader(fh, delimiter=";"):
            row_ = {slugify(k, "_"): str(v) for k, v in record.items() if v is not None}
            row = {k: v for k, v in row_.items() if k is not None}
            crawl_item(row, context)
