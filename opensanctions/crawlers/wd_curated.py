import io
import csv
import requests
from nomenklatura.resolver import Identifier

from opensanctions.core import Context
from opensanctions.wikidata import get_entity, entity_to_ftm


def crawl_row(context, row):
    qid = row.get("qid", "").strip()
    if not len(qid):
        return
    if not Identifier.QID.match(qid):
        context.log.warning("No valid QID", qid=qid)
        return
    schema = row.get("schema") or "Person"
    topics = [t.strip() for t in row.get("topics", "").split(";")]
    topics = [t for t in topics if len(t)]
    data = get_entity(context, qid)
    if data is None:
        return
    proxy = entity_to_ftm(context, data, schema=schema, topics=topics)
    context.log.info("Curated entity", entity=proxy)


def crawl(context: Context):
    text = context.fetch_text(context.dataset.data.url)
    for row in csv.DictReader(io.StringIO(text)):
        crawl_row(context, row)
