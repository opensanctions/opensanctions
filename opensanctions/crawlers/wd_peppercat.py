import io
import csv

from opensanctions.core import Context
from opensanctions.wikidata import get_entity, entity_to_ftm


def crawl_qid(context, qid, country):
    if qid is None:
        return
    data = get_entity(context, qid)
    if data is not None:
        entity = entity_to_ftm(
            context,
            data,
            position=data.get("position"),
            topics="role.pep",
            country=country,
        )
        context.log.info("Target entity", entity=entity)


def crawl(context: Context):
    text = context.fetch_text(context.dataset.data.url)
    for row in csv.DictReader(io.StringIO(text)):
        qid = row.get("personID")
        country = row.get("catalog")
        crawl_qid(context, qid, country)
