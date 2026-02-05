import csv
from typing import Dict
from rigour.mime.types import CSV

from zavod import Context
from zavod import helpers as h

CAATSA_URL = "https://prod-upp-image-read.ft.com/40911a30-057c-11e8-9650-9c0ad2d7c5b5"


def crawl_row(context: Context, row: Dict[str, str]):
    qid_raw = row.get("qid", "").strip()
    qid = h.deref_wikidata_id(context, qid_raw)
    if qid is None:
        context.log.warning("No valid QID", qid=qid_raw)
        return
    if row.get("left_russia") == "yes":
        return
    entity = context.make("Person")
    entity.id = qid
    entity.add("name", row.pop("name"), lang="eng")
    entity.add("topics", row.pop("topics", "").split(";"))
    if row.pop("caatsa2018") == "yes":
        msg = "Mentioned in 2018 CAATSA report on Russian oligarchs"
        entity.add("notes", msg, lang="eng")
        entity.add("sourceUrl", CAATSA_URL)
    # context.inspect(entity)
    context.emit(entity)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
