import csv
from pantomime.types import CSV

from opensanctions.core import Context
from opensanctions.wikidata import get_entity, entity_to_ftm
from opensanctions.wikidata.api import load_labels


def crawl_qid(context, qid, country):
    if qid is None:
        return
    data = get_entity(context, qid)
    if data is not None:
        entity_to_ftm(
            context,
            data,
            position=data.get("position"),
            topics="role.pep",
            country=country,
        )
        # context.log.info("Target entity", entity=entity)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.dataset.data.url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    load_labels(context)
    prev_country = None
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            qid = row.get("personID")
            country = row.get("catalog")
            if country != prev_country:
                context.log.info("Crawl country", country=country)
                prev_country = country
            crawl_qid(context, qid, country)
