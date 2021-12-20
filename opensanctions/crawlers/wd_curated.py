import csv
from nomenklatura.resolver import Identifier

from opensanctions import settings
from opensanctions.core import Context
from opensanctions.wikidata import get_entity, entity_to_ftm


def crawl(context: Context):
    params = {"_": settings.RUN_TIME}
    res = context.http.get(context.dataset.data.url, params=params, stream=True)
    lines = (line.decode("utf-8") for line in res.iter_lines())
    for row in csv.DictReader(lines):
        qid = row.get("qid").strip()
        if not len(qid):
            continue
        if not Identifier.QID.match(qid):
            context.log.warning("No valid QID", qid=qid)
            continue
        schema = row.get("schema") or "Person"
        topics = [t.strip() for t in row.get("topics", "").split(";")]
        topics = [t for t in topics if len(t)]
        data = get_entity(qid)
        entity_to_ftm(context, data, schema=schema, topics=topics)
