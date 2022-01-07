import io
import csv
from nomenklatura.resolver import Identifier

from opensanctions import settings
from opensanctions.core import Context
from opensanctions.wikidata import get_entity, entity_to_ftm


async def crawl(context: Context):
    text = await context.fetch_text(context.dataset.data.url)
    for row in csv.DictReader(io.StringIO(text)):
        print(text)
        qid = row.get("qid").strip()
        if not len(qid):
            continue
        if not Identifier.QID.match(qid):
            context.log.warning("No valid QID", qid=qid)
            continue
        schema = row.get("schema") or "Person"
        topics = [t.strip() for t in row.get("topics", "").split(";")]
        topics = [t for t in topics if len(t)]
        data = await get_entity(context, qid)
        if data is None:
            continue
        await entity_to_ftm(context, data, schema=schema, topics=topics)
