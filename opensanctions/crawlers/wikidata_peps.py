import csv
from typing import Optional
from pantomime.types import CSV
from nomenklatura.util import is_qid

from opensanctions.core import Context


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.source.data.url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            # country = row.get("catalog")
            # if country != prev_country:
            #     context.log.info("Crawl country", country=country)
            #     prev_country = country
            entity = context.make("Person")
            qid: Optional[str] = row.get("person_qid")
            if qid is None or not is_qid(qid):
                continue
            entity.id = qid
            entity.add("name", row.get("person_qid"))
            entity.add("topics", "role.pep")
            # entity.add("country", country)
            context.emit(entity, target=True)
