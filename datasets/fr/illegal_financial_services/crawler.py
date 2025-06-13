import csv
import re
from typing import Dict
from rigour.mime.types import CSV
from normality import slugify

from zavod import Context, helpers as h

URL_PATTERN = (
    r"^(https?:\/\/)?"  # Match the scheme (http or https)
    r"(([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,})"  # Match domain
    r"(\/[a-zA-Z0-9-._~:\/?#\[\]@!$&\'()*+,;%=]*)?"  # Match path, query string, fragment
    r"(\.[a-zA-Z]{2,})?$"  # Match top-level domain
)

EMAIL_PATTERN = (
    r"^[a-zA-Z0-9_.+-]+"  # Local part before the @ symbol
    r"@[a-zA-Z0-9-]+"  # Domain part after the @ symbol
    r"\.[a-zA-Z0-9-.]+$"  # Top-level domain
)


def crawl_item(row: Dict[str, str], context: Context):
    entity = context.make("LegalEntity")
    entity.id = context.make_id(row.get("nom"))
    entity.add("name", row.get("nom"))
    entity.add("topics", "crime.fin")

    if re.match(URL_PATTERN, row.get("nom")):
        entity.add("website", row.get("nom"))
    elif re.match(EMAIL_PATTERN, row.get("nom")):
        entity.add("email", row.get("nom"))

    row.pop("nom")

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
