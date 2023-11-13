import csv
from typing import Optional, Any
from pantomime.types import CSV
from nomenklatura.util import is_qid

from zavod import Context
from zavod import helpers as h
from zavod.util import remove_emoji


DECISIONS = {
    "subnational": "State government",
    "national": "National government",
    "international": "International organization",
}
TOPICS = {
    "subnational": ["gov.state"],
    "national": ["gov.national"],
    "international": ["gov.igo"],
}


def date_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if len(text) < 4:
        return None
    return text[:10]


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            entity = context.make("Person")
            qid: Optional[str] = row.get("person_qid")
            if not is_qid(qid) or qid == "Q1045488":
                continue
            entity.id = qid

            keyword = DECISIONS.get(row.get("decision", ""))
            if keyword is None:
                continue

            position_qid = row.get("position_qid")
            position_label = remove_emoji(row.get("position_label"))
            if not position_label:
                position_label = position_qid

            position = h.make_position(
                context,
                position_label,
                country=row.get("country_code"),
                topics=TOPICS.get(row.get("decision", ""), []),
                wikidata_id=position_qid,
            )
            occupancy = h.make_occupancy(
                context,
                entity,
                position,
                False,
                death_date=date_value(row.get("person_death")),
                birth_date=date_value(row.get("person_birth")),
                end_date=date_value(row.get("end_date")),
                start_date=date_value(row.get("start_date")),
            )
            if not occupancy:
                continue

            res = context.lookup("role_topics", position_label)
            if res:
                position.add("topics", res.topics)

            # TODO: decide all entities with no P39 dates as false?
            # print(holder.person_qid, death, start_date, end_date)

            if row.get("person_label") != qid:
                entity.add("name", row.get("person_label"))
            entity.add("keywords", keyword)

            context.emit(position)
            context.emit(occupancy)
            context.emit(entity, target=True)

    entity = context.make("Person")
    entity.id = "Q21258544"
    entity.add("name", "Mark Lipparelli")
    entity.add("topics", "role.pep")
    entity.add("country", "us")
    context.emit(entity, target=True)
