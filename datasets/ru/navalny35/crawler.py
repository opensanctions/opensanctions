import csv
from typing import Dict
from rigour.mime.types import CSV
from rigour.ids.wikidata import is_qid

from zavod import Context


def crawl_row(context: Context, row: Dict[str, str]):
    qid = row.get("qid", "").strip()
    if not len(qid):
        return
    if not is_qid(qid):
        context.log.warning("No valid QID", qid=qid)
        return
    schema = row.get("schema") or "Person"
    entity = context.make(schema)
    entity.id = qid
    entity.add("wikidataId", qid)
    topics = [t.strip() for t in row.get("topics", "").split(";")]
    topics = [t for t in topics if len(t)]
    entity.add("topics", topics)
    context.emit(entity)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
