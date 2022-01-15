import io
import csv

from opensanctions.core import Context
from opensanctions.wikidata import get_entity, entity_to_ftm


async def crawl_qid(context, qid, country):
    if qid is None:
        return
    data = await get_entity(context, qid)
    if data is not None:
        entity = await entity_to_ftm(
            context,
            data,
            position=data.get("position"),
            topics="role.pep",
            country=country,
        )
        context.log.info("Target entity", entity=entity)


async def crawl(context: Context):
    text = await context.fetch_text(context.dataset.data.url)
    for row in csv.DictReader(io.StringIO(text)):
        qid = row.get("personID")
        country = row.get("catalog")
        await crawl_qid(context, qid, country)
