import csv
from typing import Dict, Optional, Any
from pantomime.types import CSV
from nomenklatura.util import is_qid

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import PositionCategorisation, categorise, get_positions
from zavod.util import remove_emoji

from zavod.shed.wikidata.query import run_query, CACHE_MEDIUM


def keyword(topics: [str]) -> Optional[str]:
    if "gov.national" in topics:
        return "National government"
    if "gov.state" in topics:
        return "State government"
    if "gov.igo" in topics:
        return "International organization"
    return None


def date_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if len(text) < 4:
        return None
    return text[:10]


def truncate_date(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    return text[:10]


def crawl_holder(context: Context, categorisation, holder: Dict[str, str]) -> None:
    print(holder)
    entity = context.make("Person")
    qid: Optional[str] = holder.get("person_qid")
    if not is_qid(qid) or qid == "Q1045488":
        return
    entity.id = qid

    position_qid = categorisation.entity_id
    position_label = remove_emoji(categorisation.caption)
    if not position_label:
        position_label = position_qid

    position = h.make_position(
        context,
        position_label,
        country=categorisation.countries,
        topics=categorisation.topics,
        wikidata_id=categorisation.entity_id,
    )
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

    # TODO: decide all entities with no P39 dates as false?
    # print(holder.person_qid, death, start_date, end_date)

    if holder.get("person_label") != qid:
        entity.add("name", holder.get("person_label"))
    entity.add("keywords", keyword(categorisation.topics))

    context.emit(position)
    context.emit(occupancy)
    context.emit(entity, target=True)


def query_position_holders(context: Context, categorisation: PositionCategorisation) -> None:
    vars = {"POSITION": categorisation.entity_id}
    context.log.info("Crawling position [%s]: %s", categorisation.entity_id, categorisation.caption)
    response = run_query(context, "holders/holders", vars, cache_days=CACHE_MEDIUM)
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
        
        yield {
            "person_qid": binding.plain("person"),
            "person_label": binding.plain("personLabel"),
            "person_description": binding.plain("personDescription"),
            "person_birth": truncate_date(binding.plain("birth")),
            "person_death": truncate_date(binding.plain("death")),
            "start_date": start_date,
            "end_date": end_date,
            "position_qid": categorisation.entity_id,
            "position_label": categorisation.caption,
            "country": categorisation.countries,
        }


def crawl(context: Context):
    for categorisation in get_positions(context, dataset="wd_pep", is_pep=True):
        print(categorisation.caption)
        for holder in query_position_holders(context, categorisation):
            crawl_holder(context, categorisation, holder)

    entity = context.make("Person")
    entity.id = "Q21258544"
    entity.add("name", "Mark Lipparelli")
    entity.add("topics", "role.pep")
    entity.add("country", "us")
    context.emit(entity, target=True)


