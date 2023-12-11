import csv
from typing import Dict, Optional, Any
from pantomime.types import CSV
from nomenklatura.util import is_qid

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise
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


def crawl_holder(context: Context, holder: Dict[str, str]) -> None:
    entity = context.make("Person")
    qid: Optional[str] = holder.get("person_qid")
    if not is_qid(qid) or qid == "Q1045488":
        return
    entity.id = qid

    keyword = DECISIONS.get(holder.get("decision", ""))
    if keyword is None:
        return

    position_qid = holder.get("position_qid")
    position_label = remove_emoji(holder.get("position_label"))
    if not position_label:
        position_label = position_qid

    position = h.make_position(
        context,
        position_label,
        country=holder.get("country_code"),
        topics=TOPICS.get(holder.get("decision", ""), []),
        wikidata_id=position_qid,
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        False,
        death_date=date_value(holder.get("person_death")),
        birth_date=date_value(holder.get("person_birth")),
        end_date=date_value(holder.get("end_date")),
        start_date=date_value(holder.get("start_date")),
        categorisation=categorisation,
    )
    if not occupancy:
        return

    res = context.lookup("role_topics", position_label)
    if res:
        position.add("topics", res.topics)

    # TODO: decide all entities with no P39 dates as false?
    # print(holder.person_qid, death, start_date, end_date)

    if holder.get("person_label") != qid:
        entity.add("name", holder.get("person_label"))
    entity.add("keywords", keyword)

    context.emit(position)
    context.emit(occupancy)
    context.emit(entity, target=True)


def query_position_holders(position: Dict[str, str]) -> None:
   
    vars = {"POSITION": position.qid}
    context.log.info("Crawling position [%s]: %s", position.qid, position.label)
    response = run_query(conn, "holders/holders", vars, expire=CACHE_MEDIUM)
    holders: Dict[str, Holder] = {}
    for binding in response.results:
        start_date = truncate_date(
            binding.plain("positionStart")
            or binding.plain("bodyStart")
            or binding.plain("bodyInception")
        )
        end_date = truncate_date(
            binding.plain("positionEnd")
            or binding.plain("bodyEnd")
            or binding.plain("bodyAbolished")
        )
        holder_id = make_id(
            binding.plain("ps"),
            binding.plain("person"),
            position.qid,
            start_date,
            end_date,
        )
        if holder_id is None:
            log.error("No holder ID: %r", binding)
            continue

        yield {
            "id": holder_id,
            "person_qid": binding.plain("person"),
            "person_label": binding.plain("personLabel"),
            "person_description": binding.plain("personDescription"),
            "person_birth": truncate_date(binding.plain("birth")),
            "person_death": truncate_date(binding.plain("death")),
            "start_date": start_date,
            "end_date": end_date,
            "position_qid": position.qid,
            "country_qid": position.country_qid,
        }


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    for position in fetch_positions(context):
        for holder in query_position_holders(context, position):
            crawl_holder(context, holder)

    entity = context.make("Person")
    entity.id = "Q21258544"
    entity.add("name", "Mark Lipparelli")
    entity.add("topics", "role.pep")
    entity.add("country", "us")
    context.emit(entity, target=True)


