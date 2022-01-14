import io
import csv
import asyncio
import requests
from nomenklatura.resolver import Identifier

from opensanctions.core import Context
from opensanctions.wikidata import get_entity, entity_to_ftm


async def crawl_row(context, row):
    qid = row.get("qid", "").strip()
    if not len(qid):
        return
    if not Identifier.QID.match(qid):
        context.log.warning("No valid QID", qid=qid)
        return
    schema = row.get("schema") or "Person"
    topics = [t.strip() for t in row.get("topics", "").split(";")]
    topics = [t for t in topics if len(t)]
    data = await get_entity(context, qid)
    if data is None:
        return
    proxy = await entity_to_ftm(context, data, schema=schema, topics=topics)
    context.log.info("Curated entity", entity=proxy)


async def crawl(context: Context):
    # text = await context.fetch_text(context.dataset.data.url)
    res = requests.get(context.dataset.data.url)
    tasks = []
    for row in csv.DictReader(io.StringIO(res.text)):
        tasks.append(crawl_row(context, row))
    await asyncio.gather(*tasks)
