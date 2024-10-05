import time
from banal import ensure_list
from collections import defaultdict
from typing import Dict, Optional, Any, List, Generator, Set
from rigour.ids.wikidata import is_qid


from nomenklatura.enrich.wikidata import WikidataEnricher

from zavod import Context, Dataset
from zavod import helpers as h
from zavod.entity import Entity
from zavod.logic.pep import PositionCategorisation, categorise
from zavod.shed.wikidata.country import all_countries, Country
from zavod.shed.wikidata.query import run_query, CACHE_MEDIUM
from zavod.shed.wikidata.human import wikidata_basic_human


DECISION_NATIONAL = "national"
RETRIES = 5


class CrawlState(object):
    def __init__(self, context: Context):
        self.ctx = context
        self.enricher: WikidataEnricher[Dataset] = WikidataEnricher(
            context.dataset, context.cache, context.dataset.config
        )
        self.log = context.log
        self.seen_positions: Set[str] = set()
        self.seen_humans: Set[str] = set()


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


def crawl_holder(
    state: CrawlState,
    categorisation: PositionCategorisation,
    position: Entity,
    holder: Dict[str, str],
) -> None:
    qid: Optional[str] = holder.get("person_qid")
    if qid is None or not is_qid(qid):
        return
    item = state.enricher.fetch_item(qid)
    if item is None:
        return
    entity = wikidata_basic_human(state.ctx, state.enricher, item, strict=False)
    if entity is None:
        return
    state.log.info(f"Crawling person {qid} ({entity.caption})")
    occupancy = h.make_occupancy(
        state.ctx,
        entity,
        position,
        False,
        death_date=max(entity.get("deathDate"), default=None),
        birth_date=max(entity.get("birthDate"), default=None),
        end_date=date_value(holder.get("end_date")),
        start_date=date_value(holder.get("start_date")),
        categorisation=categorisation,
    )
    if not occupancy:
        return

    if not len(entity.countries):
        entity.add("country", position.countries)
    # TODO: decide all entities with no P39 dates as false?
    # print(holder.person_qid, death, start_date, end_date)
    entity.add("keywords", keyword(categorisation.topics))

    state.ctx.emit(position)
    state.ctx.emit(occupancy)
    if qid not in state.seen_humans:
        state.seen_humans.add(qid)
        state.ctx.emit(entity, target=True)


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


def pick_country(context, *qids):
    """
    Returns the country for the first national decision country of the given qids, or None
    """
    for qid in qids:
        # e.g. https://www.wikidata.org/.well-known/genid/091bf3144103c0cbaca1bd6eb3762d4d
        if qid is None or not is_qid(qid):
            continue
        country = context.lookup("country_decisions", qid)
        if country is not None and country.decision == DECISION_NATIONAL:
            return country
    return None


def query_positions(
    context: Context, position_classes: List[str], country: Country
) -> Generator[Dict[str, Any], None, None]:
    """
    Yields an item for each position with all countries selected by pick_country().

    May return duplicates
    """
    context.log.info(f"Crawling positions for {country.qid} ({country.label})")
    position_countries = defaultdict(set)

    # a.1) Instances of one or more subclasses of Q4164871 (position) by jurisdiction/country
    country_results = []
    for position_class in position_classes:
        context.log.info(
            f"Querying descendants of {position_class['qid']} ({position_class['label']})"
        )
        vars = {
            "COUNTRY": country.qid,
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
        "COUNTRY": country.qid,
        "CLASS": "Q4164871",
        "RELATION": "wdt:P31",
    }
    country_response = run_query(
        context, "positions/country", vars, cache_days=CACHE_MEDIUM
    )
    country_results.extend(country_response.results)

    for bind in country_results:
        country_res = pick_country(
            context,
            bind.plain("country"),
            bind.plain("jurisdiction"),
            country.qid,
        )
        if country_res is not None:
            position_countries[bind.plain("position")].add(country.code)

    # b) Positions held by politicans from that country
    politician_response = run_query(
        context, "positions/politician", vars, cache_days=CACHE_MEDIUM
    )
    for bind in politician_response.results:
        country_res = pick_country(
            context,
            bind.plain("country"),
            bind.plain("jurisdiction"),
        )
        if country_res is not None:
            position_countries[bind.plain("position")].add(country_res.code)

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
    state = CrawlState(context)
    seen_positions = set()
    position_classes = query_position_classes(context)
    local_countries = ensure_list(context.dataset.config.get("countries_local", {}))

    for country in all_countries(context, state.enricher):
        include_local = country.code in local_countries
        context.log.info(f"Crawling country: {country.qid} ({country.label})")
        for wd_position in query_positions(context, position_classes, country):
            if wd_position["qid"] in seen_positions:
                continue

            seen_positions.add(wd_position["qid"])
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
                crawl_holder(state, categorisation, position, holder)
        context.cache.flush()

    entity = context.make("Person")
    entity.id = "Q21258544"
    entity.add("name", "Mark Lipparelli")
    entity.add("topics", "role.pep")
    entity.add("country", "us")
    context.emit(entity, target=True)
