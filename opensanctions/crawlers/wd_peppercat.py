import csv

from opensanctions import settings
from opensanctions.core import Context
from opensanctions.wikidata import get_entity, entity_to_ftm


def crawl(context: Context):
    params = {"_": settings.RUN_DATE}
    res = context.http.get(context.dataset.data.url, params=params, stream=True)
    lines = (line.decode("utf-8") for line in res.iter_lines())
    for row in csv.DictReader(lines):
        qid = row.get("personID")
        if qid is None:
            continue
        data = get_entity(qid)
        country = row.get("catalog")
        if data is not None:
            entity_to_ftm(
                context,
                data,
                position=data.get("position"),
                topics="role.pep",
                country=country,
            )
