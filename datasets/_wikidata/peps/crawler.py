import time
from collections import defaultdict
from typing import Dict, Optional, Any, List, Generator
from fingerprints import clean_brackets
from rigour.ids.wikidata import is_qid
from rigour.territories import get_territories, get_territory_by_qid

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.logic.pep import PositionCategorisation, categorise
from zavod.shed.wikidata.query import run_query, CACHE_MEDIUM

DECISION_NATIONAL = "national"
RETRIES = 5


def keyword(topics: List[str]) -> Optional[str]:
    if "gov.national" in topics:
        return "National government"
    if "gov.state" in topics:
        return "State government"
    if "gov.igo" in topics:
        return "International organization"
    if "gov.muni" in topics:
        return "Local government"
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


def pick_country(*qids: List[str]) -> Optional[str]:
    for qid in qids:
        territory = get_territory_by_qid(qid)
        if territory is not None and territory.ftm_country is not None:
            return territory.ftm_country
    return None


def crawl_holder(
    context: Context,
    categorisation: PositionCategorisation,
    position: Entity,
    holder: Dict[str, str],
) -> None:
    entity = context.make("Person")
    qid: Optional[str] = holder.get("person_qid")
    if qid is None or not is_qid(qid) or qid == "Q1045488":
        return
    entity.id = qid
    birth_date = holder.get("person_birth")
    death_date = holder.get("person_death")
    start_date = holder.get("start_date")
    end_date = holder.get("end_date")
    if any(
        (d and (d < "1900-01-01"))
        for d in [start_date, end_date, birth_date, death_date]
    ):
        # Avoid constructing a proxy which emits a warning
        # before we discard it.
        return
    if birth_date is not None and birth_date[:10].endswith("-01-01"):
        birth_date = birth_date[:4]
    entity.add("birthDate", birth_date, original_value=holder.get("person_birth"))
    entity.add("deathDate", holder.get("person_death"))

    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        False,
        end_date=end_date,
        start_date=start_date,
        categorisation=categorisation,
        propagate_country=("role.diplo" not in categorisation.topics),
    )
    if not occupancy:
        return

    # TODO: decide all entities with no P39 dates as false?
    # print(holder.person_qid, death, start_date, end_date)

    if holder.get("person_label") != qid:
        entity.add("name", clean_brackets(holder.get("person_label")).strip())
    entity.add("keywords", keyword(categorisation.topics))

    context.emit(position)
    context.emit(occupancy)
    context.emit(entity)


def query_position_holders(
    context: Context, wd_position: Dict[str, str]
) -> Generator[Dict[str, Any], None, None]:
    context.log.info(
        f"Crawling holders of position {wd_position['qid']} ({wd_position['label']})"
    )
    vars = {"POSITION": wd_position["qid"]}
    for i in range(1, RETRIES):
        try:
            response = run_query(
                context,
                "holders/holders",
                vars,
                cache_days=CACHE_MEDIUM * i,
            )
            break
        except Exception as e:
            context.log.info(
                f"Holder query failed, retrying {i}/{RETRIES}",
                error=str(e),
            )
            if i == RETRIES - 1:
                raise e
            time.sleep(i)

    for binding in response.results:
        if not is_qid(binding.plain("person")):
            continue
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
            "person_birth": truncate_date(binding.plain("birth")),
            "person_death": truncate_date(binding.plain("death")),
            "start_date": start_date,
            "end_date": end_date,
        }


def query_positions(
    context: Context, position_classes, country: Dict[str, str]
) -> Generator[Dict[str, Any], None, None]:
    """
    May return duplicates
    """
    context.log.info(f"Crawling positions for {country['qid']} ({country['label']})")
    position_countries = defaultdict(set)

    # a.1) Instances of one or more subclasses of Q4164871 (position) by jurisdiction/country
    country_results = []
    for position_class in position_classes:
        context.log.info(
            f"Querying descendants of {position_class['qid']} ({position_class['label']})"
        )
        vars = {
            "COUNTRY": country["qid"],
            "CLASS": position_class.get("qid"),
            "RELATION": "wdt:P31/wdt:P279*",
        }
        country_response = run_query(
            context,
            "positions/country",
            vars,
            cache_days=CACHE_MEDIUM,
        )
        country_results.extend(country_response.results)
    # a.2) Instances of Q4164871 (position) by jurisdiction/country
    context.log.info("Querying instances of Q4164871 (position)")
    vars = {
        "COUNTRY": country["qid"],
        "CLASS": "Q4164871",
        "RELATION": "wdt:P31",
    }
    country_response = run_query(
        context, "positions/country", vars, cache_days=CACHE_MEDIUM
    )
    country_results.extend(country_response.results)

    for bind in country_results:
        picked_country = pick_country(
            bind.plain("country"),
            bind.plain("jurisdiction"),
            country["qid"],
        )
        if picked_country is not None:
            position_countries[bind.plain("position")].add(picked_country)

    # b) Positions held by politicans from that country
    politician_response = run_query(
        context, "positions/politician", vars, cache_days=CACHE_MEDIUM
    )
    for bind in politician_response.results:
        picked_country = pick_country(
            bind.plain("country"),
            bind.plain("jurisdiction"),
        )
        if picked_country is not None:
            position_countries[bind.plain("position")].add(picked_country)

    for bind in country_results + politician_response.results:
        if not is_qid(bind.plain("position")):
            continue
        date_abolished = bind.plain("abolished")
        if date_abolished is not None and date_abolished < "2000-01-01":
            context.log.debug(f"Skipping abolished position: {bind.plain('position')}")
            continue
        yield {
            "qid": bind.plain("position"),
            "label": bind.plain("positionLabel"),
            "country_codes": position_countries[bind.plain("position")],
        }


def all_countries():
    for territory in get_territories():
        if territory.is_historical:
            continue
        code = territory.ftm_country
        if code is None:
            continue
        yield {"qid": territory.qid, "code": code, "label": territory.name}


def query_position_classes(context: Context):
    response = run_query(context, "positions/subclasses", cache_days=CACHE_MEDIUM)
    classes = []
    for binding in response.results:
        qid = binding.plain("class")
        if not is_qid(qid):
            continue
        label = binding.plain("classLabel")
        res = context.lookup("position_subclasses", qid)
        if res:
            if res.maybe_pep:
                classes.append({"qid": qid, "label": label})
        else:
            context.log.warning(f"Unknown subclass of position: '{qid}' ({label})")
    return classes


def crawl(context: Context):
    seen_positions = set()
    position_classes = query_position_classes(context)

    for country in all_countries():
        include_local = False
        context.log.info(f"Crawling country: {country['qid']} ({country['label']})")

        if country["code"] == "us":
            include_local = True

        for wd_position in query_positions(context, position_classes, country):
            if wd_position["qid"] in seen_positions:
                continue

            position = h.make_position(
                context,
                wd_position["label"],
                country=wd_position["country_codes"],
                wikidata_id=wd_position["qid"],
            )
            categorisation = categorise(context, position, is_pep=None)
            if not categorisation.is_pep:
                continue
            if not include_local and ("gov.muni" in categorisation.topics):
                continue

            for holder in query_position_holders(context, wd_position):
                crawl_holder(context, categorisation, position, holder)
            seen_positions.add(wd_position["qid"])

    entity = context.make("Person")
    entity.id = "Q21258544"
    entity.add("name", "Mark Lipparelli")
    entity.add("topics", "role.pep")
    entity.add("country", "us")
    context.emit(entity)
