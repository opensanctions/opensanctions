import csv
from io import StringIO
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlencode
from nomenklatura.wikidata import WikidataClient, Claim

from zavod import Context, Entity
from zavod import helpers as h
from zavod.shed.wikidata.position import wikidata_position
from zavod.shed.wikidata.human import wikidata_basic_human

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
# TEMP: We're starting to include municipal PEPs for specific countries
MUNI_COUNTRIES = {"us", "fr", "gb", "ru", "is"}


class CrawlState(object):
    def __init__(self, context: Context):
        self.context = context
        self.client = WikidataClient(context.cache, session=context.http)
        self.log = context.log
        self.ignore_positions: Set[str] = set()
        self.persons: Set[str] = set()
        self.person_title: Dict[str, str] = {}
        self.person_countries: Dict[str, Set[str]] = {}
        self.person_topics: Dict[str, Set[str]] = {}
        self.person_positions: Dict[str, Set[Entity]] = {}
        self.emitted_positions: Set[str] = set()


def title_name(title: str) -> str:
    return title.replace("_", " ")


def crawl_position(state: CrawlState, person: Entity, claim: Claim) -> None:
    item = state.client.fetch_item(claim.qid)
    if item is None:
        state.ignore_positions.add(claim.qid)
        return
    position = wikidata_position(state.context, state.client, item)
    if position is None:
        state.ignore_positions.add(item.id)
        return

    start_date: Optional[str] = None
    for qual in claim.qualifiers.get("P580", []):
        start_date = qual.text.text

    end_date: Optional[str] = None
    for qual in claim.qualifiers.get("P582", []):
        end_date = qual.text.text

    occupancy = h.make_occupancy(
        state.context,
        person,
        position,
        no_end_implies_current=False,
        start_date=start_date,
        end_date=end_date,
    )
    if occupancy is not None:
        state.log.info("  -> %s (%s)" % (position.first("name"), position.id))
        if position.id not in state.emitted_positions:
            state.emitted_positions.add(position.id)
            state.context.emit(position)
        state.context.emit(occupancy)

    # TODO: implement support for 'officeholder' (P1308) here


def crawl_person(state: CrawlState, qid: str) -> Optional[Entity]:
    item = state.client.fetch_item(qid)
    if item is None:
        return None
    entity = wikidata_basic_human(state.context, state.client, item, strict=True)
    if entity is None:
        return None

    for claim in item.claims:
        if claim.property == "P39":
            crawl_position(state, entity, claim)
    return entity


def find_paths(
    context: Context,
    category_title: str,
    page_title: str,
    path: Tuple[str],
    max_depth: int,
    cursor: Any = None,
) -> Set[str]:
    """
    Find all the paths between the page with `page_title` and the category with `category_title`.

    `category_title` must have `Category:` prefix.

    This is SLOWWWW - use for debugging.
    Better would be if this could be fetched using petscan
    https://github.com/magnusmanske/petscan_rs/issues/182
    """
    paths = set()
    if len(path) > max_depth:
        return paths
    query = {
        "action": "query",
        "format": "json",
        "prop": "categories",
        "titles": page_title,
    }
    if cursor is not None:
        query["clcontinue"] = cursor
    url = f"https://en.wikipedia.org/w/api.php?{urlencode(query)}"
    data = context.fetch_json(url, cache_days=7)
    pageids = list(data["query"]["pages"].keys())
    assert len(pageids) == 1
    categories = data["query"]["pages"][pageids[0]]["categories"]

    for category in categories:
        if category_title == category["title"]:
            paths.add(path + (category_title,))
        if category["title"] == "Category:Contents":
            continue
        else:
            paths.update(
                find_paths(
                    context,
                    category_title,
                    category["title"],
                    path + (category["title"],),
                    max_depth,
                )
            )
    # Paginate
    if "continue" in data:
        paths.update(
            find_paths(
                context,
                category_title,
                page_title,
                path,
                max_depth,
                cursor=data["continue"]["clcontinue"],
            )
        )

    return paths


def crawl_category(state: CrawlState, category: Dict[str, Any]) -> None:
    cache_days = int(category.pop("cache_days", 14))
    topics: List[str] = category.pop("topics", [])
    if "topic" in category:
        topics.append(category.pop("topic"))
    country: Optional[str] = category.pop("country", None)

    query = dict(QUERY)
    cat: str = category.pop("category", "")
    query["categories"] = cat.strip()
    query.update(category)
    state.log.info("Crawl category: %s" % cat)

    position_data: Dict[str, Any] = category.pop("position", {})
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
        state.persons.add(person_qid)

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
        if row.get("title") is not None:
            state.person_title[person_qid] = row.get("title")

    state.log.info(
        "PETScanning category: %s" % cat,
        topics=topics,
        results=results,
    )
    state.context.cache.flush()


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

    # TEMP: skip municipal governments
    topics = position.get("topics")
    if "gov.muni" in topics and MUNI_COUNTRIES.isdisjoint(position.countries):
        return persons

    query = f"""
    SELECT ?person WHERE {{
        ?person wdt:P39 wd:{position_qid} .
        ?person wdt:P31 wd:Q5
    }}
    """
    response = state.client.query(query)
    for result in response.results:
        person_qid = result.plain("person")
        if person_qid is not None:
            persons.add(person_qid)

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
    roles: Set[str] = set()
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
            roles.add(role)

    state.log.info("Found %d seed positions" % len(roles))
    for role in roles:
        state.persons.update(crawl_position_holder(state, role))
    state.context.cache.flush()


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
        person = result.plain("person")
        state.persons.add(person)
        if person not in state.person_topics:
            state.person_topics[person] = set()
        state.person_topics[person].add("role.pep")
        if person not in state.person_countries:
            state.person_countries[person] = set()
        state.person_countries[person].add("ru")


def crawl(context: Context) -> None:
    state = CrawlState(context)
    crawl_declarator(state)
    crawl_position_seeds(state)
    categories: List[Dict[str, Any]] = context.dataset.config.get("categories", [])
    for category in categories:
        crawl_category(state, category)
        state.context.cache.flush()

    context.log.info("Generated %d persons" % len(state.persons))
    for idx, person_qid in enumerate(state.persons):
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
            occupancy = h.make_occupancy(state.context, entity, position)
            if occupancy is not None:
                state.context.emit(occupancy)
            if position.id not in state.emitted_positions:
                state.emitted_positions.add(position.id)
                state.context.emit(position)

        state.log.info(
            "Crawled person %s: %s %r"
            % (entity.id, entity.caption, entity.get("topics"))
        )
        state.context.emit(entity)
        if idx > 0 and idx % 1000 == 0:
            state.context.cache.flush()

    # raise RuntimeError("Crawler is in debug mode, do not release results")
