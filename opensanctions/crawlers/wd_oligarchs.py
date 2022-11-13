import csv
from typing import Dict
from pantomime.types import CSV
from nomenklatura.util import is_qid

from opensanctions.core import Context


def crawl_row(context: Context, row: Dict[str, str]):
    qid = row.get("qid", "").strip()
    if not len(qid):
        return
    if not is_qid(qid):
        context.log.warning("No valid QID", qid=qid)
        return
    entity = context.make("Person")
    entity.id = qid
    entity.add("topics", "role.oligarch")
    context.emit(entity, target=True)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.source.data.url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
