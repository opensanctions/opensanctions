import csv
from typing import Dict
from rigour.mime.types import CSV

from zavod import Context
from zavod import helpers as h


def crawl_row(context: Context, row: Dict[str, str]) -> None:
    qid_raw = row.get("qid", "").strip()
    qid = h.deref_wikidata_id(context, qid_raw)
    if qid is None:
        context.log.warning("No valid QID", qid=qid_raw)
        return
    schema = row.get("schema") or "Person"
    entity = context.make(schema)
    entity.id = qid
    entity.add("wikidataId", qid)
    topics = [t.strip() for t in row.get("topics", "").split(";")]
    topics = [t for t in topics if len(t)]
    entity.add("topics", topics)
    context.emit(entity)


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
