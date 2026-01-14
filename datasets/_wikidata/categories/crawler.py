import csv
from collections import defaultdict
from dataclasses import dataclass, field
from io import StringIO
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlencode

from nomenklatura.wikidata import Claim, WikidataClient
from zavod.shed.wikidata.human import wikidata_basic_human
from nomenklatura.wikidata.value import clean_wikidata_name
from zavod.shed.wikidata.position import (
    position_holders,
    wikidata_occupancy,
    wikidata_position,
)
from zavod.stateful.positions import categorised_position_qids

from zavod import Context, Entity
from zavod import helpers as h

URL = "https://petscan.wmcloud.org/"
QUERY = {
    "doit": "",
    "depth": 4,
    # "combination": "subset",
    "format": "csv",
    "wikidata_item": "with",
    "wikidata_prop_item_use": "Q5",
    "search_max_results": 1000,
    "sortorder": "ascending",
}
# That one time a PEP customer asked to be included....
ALWAYS_PERSONS = ["Q21258544"]


@dataclass
class FoundRecord:
    from_categories: Set[str] = field(default_factory=set)
    from_positions: Set[str] = field(default_factory=set)
    from_declarator: bool = False


class CrawlState(object):
    def __init__(self, context: Context):
        self.context = context
        self.client = WikidataClient(context.cache, session=context.http)
        self.log = context.log
        self.ignore_positions: Set[str] = set()

        self.persons: Dict[str, FoundRecord] = defaultdict(FoundRecord)
        self.persons.update({qid: FoundRecord() for qid in ALWAYS_PERSONS})

        self.person_title: Dict[str, str] = {}
        self.person_countries: Dict[str, Set[str]] = {}
        self.person_topics: Dict[str, Set[str]] = {}
        self.person_positions: Dict[str, Set[Entity]] = {}
        self.emitted_positions: Set[str] = set()
        exc = [str(x) for x in context.dataset.config.get("exclusion_checks", [])]
        self.exclusion_checks: Set[str] = set(exc)


def title_name(title: str) -> Optional[str]:
    return clean_wikidata_name(title.replace("_", " "))


def crawl_position(state: CrawlState, person: Entity, claim: Claim) -> None:
    item = state.client.fetch_item(claim.qid)
    if item is None:
        if claim.qid is not None:
            state.ignore_positions.add(claim.qid)
        return
    position = wikidata_position(state.context, state.client, item)
    if position is None or position.id is None:
        state.ignore_positions.add(item.id)
        return

    occupancy = wikidata_occupancy(state.context, person, position, claim)
    if occupancy is not None:
        state.log.info("  -> %s (%s)" % (position.first("name"), position.id))
        if position.id not in state.emitted_positions:
            state.emitted_positions.add(position.id)
            state.context.emit(position)
        state.context.emit(occupancy)

    # TODO: implement support for 'officeholder' (P1308) here
    for officeholder_claim in item.claims:
        if officeholder_claim.property == "P1308":  # officeholder
            if officeholder_claim.qid is None:
                continue
            holder = crawl_person(state, officeholder_claim.qid, recurse=False)
            if holder is not None:
                occupancy = wikidata_occupancy(
                    state.context, holder, position, officeholder_claim
                )
                if occupancy is not None:
                    state.context.emit(occupancy)
                    state.context.emit(holder)


def crawl_person(state: CrawlState, qid: str, recurse: bool = True) -> Optional[Entity]:
    item = state.client.fetch_item(qid)
    if item is None:
        return None
    entity = wikidata_basic_human(state.context, state.client, item, strict=True)
    if entity is None:
        return None

    if recurse:
        for claim in item.claims:
            if claim.property == "P39":
                crawl_position(state, entity, claim)
    return entity


def crawl_category(state: CrawlState, category_crawl_spec: Dict[str, Any]) -> None:
    cache_days = int(category_crawl_spec.pop("cache_days", 14))
    topics: List[str] = category_crawl_spec.pop("topics", [])
    if "topic" in category_crawl_spec:
        topics.append(category_crawl_spec.pop("topic"))
    country: Optional[str] = category_crawl_spec.pop("country", None)

    query = dict(QUERY)
    cat: str = category_crawl_spec.pop("category", "")
    query["categories"] = cat.strip()
    query.update(category_crawl_spec)
    state.log.info("Crawl category: %s" % cat)

    position_data: Dict[str, Any] = category_crawl_spec.pop("position", {})
    position: Optional[Entity] = None
    if "name" in position_data:
        position = h.make_position(
            state.context, **position_data, id_hash_prefix="wd-cat"
        )

    query_string = urlencode(query)
    url = f"{URL}?{query_string}"
    data = state.context.fetch_text(url, cache_days=cache_days)
    wrapper = StringIO(data)
    results = 0
    for row in csv.DictReader(wrapper):
        results += 1
        person_qid = row["Wikidata"]
        if person_qid is None:
            continue
        if person_qid in state.exclusion_checks:
            state.context.log.warning("Regression on exclusion found", qid=person_qid)
            continue
        state.persons[person_qid].from_categories.add(cat)

        if person_qid not in state.person_topics:
            state.person_topics[person_qid] = set()
        state.person_topics[person_qid].update(topics)
        if person_qid not in state.person_countries:
            state.person_countries[person_qid] = set()
        if country is not None:
            state.person_countries[person_qid].add(country)
        if person_qid not in state.person_positions:
            state.person_positions[person_qid] = set()
        if position is not None:
            state.person_positions[person_qid].add(position)

        person_title = row.get("title")
        if isinstance(person_title, str):
            state.person_title[person_qid] = person_title

    state.log.info(
        "PETScanning category: %s" % cat,
        topics=topics,
        results=results,
    )
    state.context.flush()


def crawl_position_holder(state: CrawlState, position_qid: str) -> Set[str]:
    persons: Set[str] = set([])
    if position_qid in state.ignore_positions:
        return persons
    item = state.client.fetch_item(position_qid)
    if item is None:
        state.ignore_positions.add(position_qid)
        return persons
    position = wikidata_position(state.context, state.client, item)
    if position is None:
        state.ignore_positions.add(position_qid)
        return persons

    persons = position_holders(state.client, item)
    for claim in item.claims:
        if claim.property == "P1308":  # officeholder
            if claim.qid is not None:
                persons.add(claim.qid)

    state.log.info(
        "Found %d holders of %s [%s]" % (len(persons), item.label, position_qid)
    )
    return persons


def crawl_position_seeds(state: CrawlState) -> None:
    seeds: List[str] = state.context.dataset.config.get("seeds", [])
    roles: Set[str] = set(categorised_position_qids(state.context))
    for seed in seeds:
        query = f"""
        SELECT ?role WHERE {{
            ?role (wdt:P279|wdt:P31)+ wd:{seed}
        }}
        """
        roles.add(seed)
        response = state.client.query(query)
        for result in response.results:
            role = result.plain("role")
            if role is not None:
                roles.add(role)

    state.log.info("Found %d seed positions" % len(roles))
    for role in roles:
        for position_holder_qid in crawl_position_holder(state, role):
            state.persons[position_holder_qid].from_positions.add(role)

        state.context.flush()


def crawl_declarator(state: CrawlState) -> None:
    # Import all profiles which have a Declarator ID, a reference to a Russian PEP
    # site containing profiles of all elected officials in Russia.
    query = """
    SELECT ?person WHERE {
        ?person wdt:P1883 ?value .
        ?person wdt:P31 wd:Q5
    }
    """
    response = state.client.query(query)
    state.log.info("Found %d declarator profiles" % len(response.results))
    for result in response.results:
        person_qid = result.plain("person")
        if person_qid is None:
            continue
        state.persons[person_qid] = FoundRecord(from_declarator=True)
        if person_qid not in state.person_topics:
            state.person_topics[person_qid] = set()
        state.person_topics[person_qid].add("role.pep")
        if person_qid not in state.person_countries:
            state.person_countries[person_qid] = set()
        state.person_countries[person_qid].add("ru")


def crawl_persons(state: CrawlState) -> None:
    state.context.log.info("Generated %d persons" % len(state.persons))
    for idx, (person_qid, found_record) in enumerate(state.persons.items()):
        entity = crawl_person(state, person_qid)
        if entity is None:
            continue

        if not entity.has("name") and person_qid in state.person_title:
            name = title_name(state.person_title[person_qid])
            entity.add("name", name)
        entity.add("topics", state.person_topics.get(person_qid, []))

        if not len(entity.countries):
            entity.add("country", state.person_countries.get(person_qid, []))

        positions = state.person_positions.get(person_qid, [])
        for position in positions:
            if position.id is None:
                continue
            occupancy = h.make_occupancy(state.context, entity, position)
            if occupancy is not None:
                state.context.emit(occupancy)
            if position.id not in state.emitted_positions:
                state.emitted_positions.add(position.id)
                state.context.emit(position)

        state.log.info(
            f"Crawled person {entity.id} "
            f"(found in categories {found_record.from_categories}, positions {found_record.from_positions}): "
            f"{entity.caption} {entity.get('topics')}"
        )

        # Some categories we crawl but don't assign topics to. We do this for two reasons:
        #   1. We only want to discover interesting positions in the category (which then trigger at
        #      topic assignment), but there are more persons in the category not relevant to us.
        #   2. We want to discover QIDs for enrichment, just in case the Wikidata enricher (which only searches
        #      for a single name) doesn't find them. In this case, the xref process will discover the QID for the
        #      cluster (but not actually pull in the statements marked as external).
        #  In both cases, we want to emit the statements as external.
        has_topics = len(entity.get("topics")) > 0
        state.context.emit(entity, external=(not has_topics))

        if idx > 0 and idx % 1000 == 0:
            state.context.flush()

    # raise RuntimeError("Crawler is in debug mode, do not release results")


def crawl(context: Context) -> None:
    state = CrawlState(context)
    crawl_declarator(state)
    crawl_position_seeds(state)
    category_crawl_specs: List[Dict[str, Any]] = context.dataset.config.get(
        "categories", []
    )
    for category_crawl_spec in category_crawl_specs:
        crawl_category(state, category_crawl_spec)
        state.context.flush()

    crawl_persons(state)
