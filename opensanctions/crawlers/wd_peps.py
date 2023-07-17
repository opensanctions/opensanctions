import csv
from functools import cache
from typing import Optional, Dict, Any
from datetime import timedelta
from pantomime.types import CSV
from nomenklatura.util import is_qid

from zavod import settings
from opensanctions.core import Context

DECISIONS = {
    "subnational": "State government",
    "national": "National government",
    "international": "International organization",
}


YEAR = 365  # days
AFTER_OFFICE = 5 * YEAR
AFTER_DEATH = 5 * YEAR
MAX_AGE = 110 * YEAR
MAX_OFFICE = 40 * YEAR


@cache
def to_date(days: int) -> str:
    dt = settings.RUN_TIME - timedelta(days=days)
    return dt.isoformat()[:10]


def date_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if len(text) < 4:
        return None
    return text[:10]


def check_qualified(row: Dict[str, Any]) -> bool:
    # TODO: PEP logic
    death = date_value(row.get("person_death"))
    if death is not None and to_date(AFTER_DEATH) > death:
        return False

    birth = date_value(row.get("person_birth"))
    if birth is not None and to_date(MAX_AGE) > birth:
        return False

    end_date = date_value(row.get("end_date"))
    if end_date is not None and to_date(AFTER_OFFICE) > end_date:
        return False

    start_date = date_value(row.get("start_date"))
    if start_date is not None and to_date(MAX_OFFICE) > start_date:
        return False

    # TODO: decide all entities with no P39 dates as false?
    # print(holder.person_qid, death, start_date, end_date)
    has_date = (
        death is not None
        or birth is not None
        or end_date is not None
        or start_date is not None
    )
    return has_date


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.source.data.url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            entity = context.make("Person")
            qid: Optional[str] = row.get("person_qid")
            if qid is None or not is_qid(qid):
                continue
            if not check_qualified(row):
                continue
            keyword = DECISIONS.get(row.get("decision", ""))
            if keyword is None:
                continue
            entity.id = qid
            if row.get("person_label") != qid:
                entity.add("name", row.get("person_label"))
            entity.add("keywords", keyword)
            entity.add("topics", "role.pep")
            entity.add("country", row.get("country_code"))
            context.emit(entity, target=True)

    entity = context.make("Person")
    entity.id = "Q21258544"
    entity.add("name", "Mark Lipparelli")
    entity.add("topics", "role.pep")
    entity.add("country", "us")
    context.emit(entity, target=True)
