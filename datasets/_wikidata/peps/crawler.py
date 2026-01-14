from collections import defaultdict
from typing import Optional, List, Generator, NamedTuple, Set
from rigour.ids.wikidata import is_qid
from rigour.territories import get_territories, get_territory_by_qid
from nomenklatura.wikidata import WikidataClient, SparqlBinding

from zavod import Context
from zavod.entity import Entity
from zavod.shed.wikidata.human import wikidata_basic_human
from zavod.shed.wikidata.position import wikidata_occupancy, wikidata_position
from zavod.shed.wikidata.position import position_holders


class Country(NamedTuple):
    qid: str
    code: str
    label: Optional[str]


class Position(NamedTuple):
    qid: str
    label: Optional[str]
    country_codes: Set[str]


def pick_country(*qids: Optional[str]) -> Optional[str]:
    for qid in qids:
        if qid is None:
            continue
        territory = get_territory_by_qid(qid)
        if territory is not None and territory.ftm_country is not None:
            return territory.ftm_country
    return None


def crawl_holder(
    context: Context,
    client: WikidataClient,
    position: Entity,
    person_qid: str,
) -> Optional[Entity]:
    if not is_qid(person_qid):
        return None
    item = client.fetch_item(person_qid)
    if item is None:
        return None
    entity = wikidata_basic_human(context, client, item)
    if entity is None:
        return None

    has_occupancy = False
    for claim in item.claims:
        if claim.property == "P39" and claim.qid == position.id:
            occupancy = wikidata_occupancy(
                context,
                entity,
                position,
                claim,
            )
            if occupancy is not None:
                context.emit(occupancy)
                has_occupancy = True

    if not has_occupancy:
        return None

    context.emit(entity)
    return entity


def query_positions(
    context: Context,
    client: WikidataClient,
    position_classes: List[Position],
    country: Country,
) -> Generator[Position, None, None]:
    """
    May return duplicates
    """
    context.log.info(f"Crawling positions for {country.qid} ({country.label})")
    position_countries: defaultdict[str, Set[str]] = defaultdict(set)

    # a.1) Instances of one or more subclasses of Q4164871 (position) by jurisdiction/country
    country_results: List[SparqlBinding] = []
    for position_class in position_classes:
        context.log.info(
            f"Querying descendants of {position_class.qid} ({position_class.label!r}) in {country.label!r}"
        )
        country_query = f"""
        SELECT ?position ?positionLabel ?country ?jurisdiction ?abolished WHERE {{
            {{ SELECT ?position WHERE {{ ?position (wdt:P31|wdt:P279)* wd:{position_class.qid} . }} }}
            {{ SELECT ?position WHERE {{ ?position wdt:P1001|wdt:P17 wd:{country.qid} . }} }}
            OPTIONAL {{ ?position wdt:P17 ?country }}
            OPTIONAL {{ ?position wdt:P1001 ?jurisdiction }}
            OPTIONAL {{ ?position p:P576|p:P582 [ a wikibase:BestRank ; psv:P576|psv:P582 [ wikibase:timeValue ?abolished ] ] }}
            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,de,es,fr,ru,*". }}
        }}
        GROUP BY ?position ?positionLabel ?country ?jurisdiction ?abolished
        """
        country_response = client.query(country_query)
        country_results.extend(country_response.results)

    # a.2) Instances of Q4164871 (position) by jurisdiction/country
    context.log.info("Querying instances of Q4164871 (position)")
    country_query = f"""
        SELECT ?position ?positionLabel ?country ?jurisdiction ?abolished WHERE {{
            {{ SELECT ?position WHERE {{ ?position wdt:P31* wd:Q4164871 . }} }}
            {{ SELECT ?position WHERE {{ ?position wdt:P1001|wdt:P17 wd:{country.qid} . }} }}
            OPTIONAL {{ ?position wdt:P17 ?country }}
            OPTIONAL {{ ?position wdt:P1001 ?jurisdiction }}
            OPTIONAL {{ ?position p:P576|p:P582 [ a wikibase:BestRank ; psv:P576|psv:P582 [ wikibase:timeValue ?abolished ] ] }}
            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,de,es,fr,ru,*". }}
        }}
        GROUP BY ?position ?positionLabel ?country ?jurisdiction ?abolished
        """
    country_response = client.query(country_query)
    country_results.extend(country_response.results)

    for bind in country_results:
        picked_country = pick_country(
            bind.plain("country"),
            bind.plain("jurisdiction"),
            country.qid,
        )
        position = bind.plain("position")
        if picked_country is not None and position is not None:
            position_countries[position].add(picked_country)

    # b) Positions held by politicans from that country
    # occupation (P106) == politician (Q82955)
    # country of citizenship (P27) == country.qid
    politician_query = f"""
        SELECT ?position ?positionLabel ?jurisdiction ?country ?abolished
        WHERE {{
            ?holder wdt:P39 ?position .
            ?holder wdt:P106 wd:Q82955 .
            ?holder wdt:P27 wd:{country.qid} .
            OPTIONAL {{ ?position wdt:P1001 ?jurisdiction }}
            OPTIONAL {{ ?position wdt:P17 ?country }}
            OPTIONAL {{ ?position p:P576|p:P582 [ a wikibase:BestRank ; psv:P576|psv:P582 [ wikibase:timeValue ?abolished ] ] }}
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,de,es,fr,ru,*". }}
        }}
        GROUP BY ?position ?positionLabel ?jurisdiction ?country ?abolished
    """
    politician_response = client.query(politician_query)
    for bind in politician_response.results:
        picked_country = pick_country(
            bind.plain("country"),
            bind.plain("jurisdiction"),
        )
        position = bind.plain("position")
        if picked_country is not None and position is not None:
            position_countries[position].add(picked_country)

    for bind in country_results + politician_response.results:
        position = bind.plain("position")
        if position is None or not is_qid(position):
            continue
        date_abolished = bind.plain("abolished")
        if date_abolished is not None and date_abolished < "2000-01-01":
            context.log.debug(f"Skipping abolished position: {bind.plain('position')}")
            continue
        yield Position(
            position,
            bind.plain("positionLabel"),
            position_countries[position],
        )


def all_countries() -> Generator[Country, None, None]:
    for territory in get_territories():
        if territory.is_historical:
            continue
        code = territory.ftm_country
        if code is None:
            continue
        yield Country(territory.qid, code, territory.name)


def query_position_classes(context: Context, client: WikidataClient) -> List[Position]:
    subclasses_query = """
    SELECT ?class ?classLabel WHERE {
        ?class wdt:P279 wd:Q4164871 .
        SERVICE wikibase:label { bd:serviceParam wikibase:language "en,de,es,fr,ru,*". }
    }
    """
    response = client.query(subclasses_query)
    classes: List[Position] = []
    for binding in response.results:
        qid = binding.plain("class")
        if qid is None or not is_qid(qid):
            continue
        label = binding.plain("classLabel")
        res = context.lookup("position_subclasses", qid)
        if res:
            if res.maybe_pep:
                classes.append(Position(qid, label, set()))
        else:
            context.log.warning(f"Unknown subclass of position: '{qid}' ({label})")
    return classes


def crawl(context: Context):
    seen_positions: Set[str] = set()
    cache_days = context.dataset.config.get("cache_days", 14)
    client = WikidataClient(context.cache, context.http, cache_days=cache_days)
    position_classes = query_position_classes(context, client)

    for country in all_countries():
        context.log.info(f"Crawling country: {country.qid} ({country.label})")

        for wd_position in query_positions(context, client, position_classes, country):
            if wd_position.qid in seen_positions:
                continue

            seen_positions.add(wd_position.qid)

            pos_item = client.fetch_item(wd_position.qid)
            if pos_item is None:
                continue

            position = wikidata_position(context, client, pos_item)
            if position is None:
                continue

            context.log.info("Position [%s]: %s" % (position.id, position.caption))

            has_holders = False
            for person in position_holders(client, pos_item):
                holder = crawl_holder(context, client, position, person)
                if holder is not None:
                    has_holders = True

            if has_holders:
                context.emit(position)

            context.flush()
