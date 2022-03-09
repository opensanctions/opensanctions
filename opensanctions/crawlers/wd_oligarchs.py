import csv
from pantomime.types import CSV
from nomenklatura.util import is_qid

from opensanctions.core import Context
from opensanctions.wikidata import get_entity, entity_to_ftm


def crawl_row(context, row):
    qid = row.get("qid", "").strip()
    if not len(qid):
        return
    if not is_qid(qid):
        context.log.warning("No valid QID", qid=qid)
        return
    schema = "Person"
    # topics = [t.strip() for t in row.get("topics", "").split(";")]
    # topics = [t for t in topics if len(t)]
    topics = ["poi"]
    data = get_entity(context, qid)
    if data is None:
        return
    proxy = entity_to_ftm(context, data, schema=schema, topics=topics, depth=2)
    context.log.info("Curated entity", entity=proxy)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.dataset.data.url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
