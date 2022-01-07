import io
import csv

from opensanctions import settings
from opensanctions.core import Context
from opensanctions.wikidata import get_entity, entity_to_ftm


async def crawl(context: Context):
    text = await context.fetch_text(context.dataset.data.url)
    for row in csv.DictReader(io.StringIO(text)):
        qid = row.get("personID")
        if qid is None:
            continue
        data = await get_entity(context, qid)
        country = row.get("catalog")
        if data is not None:
            await entity_to_ftm(
                context,
                data,
                position=data.get("position"),
                topics="role.pep",
                country=country,
            )
