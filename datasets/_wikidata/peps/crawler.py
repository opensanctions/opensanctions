from collections.abc import Generator
from datetime import datetime
from functools import lru_cache

from nomenklatura.wikidata import Item, WikidataClient
from rigour.ids.wikidata import is_qid
from rigour.territories import get_territories
from rigour.territories.territory import Territory
from zavod.entity import Entity
from zavod.shed.wikidata.client import WIKIDATA_QUERY_CACHE, create_wikidata_client
from zavod.shed.wikidata.human import wikidata_basic_human
from zavod.shed.wikidata.igo import INTL_ORGS
from zavod.shed.wikidata.position import (
    POSITION_ABOLISHED_CUTOFF,
    position_holders,
    wikidata_occupancy,
    wikidata_position,
)
from zavod.stateful.positions import categorised_position_qids

from zavod import Context

# Positions are discovered by evidence of use — someone holds them via P39
# (position held), or they name an officeholder via P1308 — rather than by
# traversing wikidata's chaotic ontology of position classes. The class
# hierarchy is consulted only upward, via `Item.types`, when classifying each
# candidate in `wikidata_position`.

ABOLISHED_CLAUSE = """
    OPTIONAL { ?position p:P576|p:P582 [ a wikibase:BestRank ; psv:P576|psv:P582 [ wikibase:timeValue ?abolished ] ] }
"""
POSITION_CACHE_SIZE = 250_000
HOLDER_QUERY_ATTEMPTS = 2


def all_territories() -> Generator[Territory, None, None]:
    """Territories to sweep for positions: countries, but also subnational
    jurisdictions (states, provinces) and pseudo-territories, as long as they
    can eventually be mapped onto an FtM country code."""
    for territory in get_territories():
        if territory.is_historical:
            continue
        if territory.ftm_country is None:
            continue
        yield territory


def collect_positions(context: Context, client: WikidataClient, query: str) -> set[str]:
    positions: set[str] = set()
    response = client.query(query, cache_days=WIKIDATA_QUERY_CACHE)
    for bind in response.results:
        position_qid = bind.plain("position")
        if position_qid is None or not is_qid(position_qid):
            continue
        date_abolished = bind.plain("abolished")
        if date_abolished is not None and date_abolished < POSITION_ABOLISHED_CUTOFF:
            context.log.debug(f"Skipping abolished position: {position_qid}")
            continue
        positions.add(position_qid)
    return positions


def query_usage_positions(
    context: Context, client: WikidataClient, territory: Territory
) -> set[str]:
    """Positions in use that are tied to the territory via jurisdiction (P1001)
    or country (P17): they either have a human holder (P39 inverse) or name an
    officeholder themselves (P1308).

    All the territory's QIDs are swept, not just the primary one: aliases cover
    entities like the Russia/Belarus Union State (an alias of `ru`), whose organ
    positions would otherwise be invisible to jurisdiction-based discovery.

    One query per QID: batching them into a single `VALUES` block makes the
    query planner give up on large territories — the six QIDs of `gb` together
    exceed the query service's own execution limit, while each on its own is
    comfortably inside it."""
    positions: set[str] = set()
    for qid in sorted(territory.qids):
        query = f"""
        SELECT DISTINCT ?position ?abolished WHERE {{
            ?position wdt:P1001|wdt:P17 wd:{qid} .
            {{ ?holder wdt:P39 ?position . ?holder wdt:P31 wd:Q5 . }}
            UNION {{ ?position wdt:P1308 ?officeholder . }}
            {ABOLISHED_CLAUSE}
        }}
        """
        positions.update(collect_positions(context, client, query))
    return positions


def query_occupation_positions(
    context: Context, client: WikidataClient, territory: Territory
) -> set[str]:
    """Positions held by politicians, diplomats and judges who are citizens of
    the country. This catches positions with no country/jurisdiction statement
    of their own, including diplomatic postings and foreign or IGO offices."""
    query = f"""
    SELECT DISTINCT ?position ?abolished WHERE {{
        ?holder wdt:P39 ?position .
        ?holder wdt:P27 wd:{territory.qid} .
        ?holder wdt:P106 ?occupation .
        VALUES ?occupation {{ wd:Q82955 wd:Q193391 wd:Q16533 }}
        {ABOLISHED_CLAUSE}
    }}
    """
    return collect_positions(context, client, query)


def query_igo_positions(context: Context, client: WikidataClient) -> set[str]:
    """Positions in use at registered international bodies, linked via P2389
    (organization directed by the office) or P361 (part of). These bodies have
    no territory, so the per-territory sweeps never see their positions."""
    values = " ".join(f"wd:{qid}" for qid in sorted(INTL_ORGS))
    query = f"""
    SELECT DISTINCT ?position ?abolished WHERE {{
        ?position wdt:P2389|wdt:P361 ?org .
        VALUES ?org {{ {values} }}
        {{ ?holder wdt:P39 ?position . ?holder wdt:P31 wd:Q5 . }}
        UNION {{ ?position wdt:P1308 ?officeholder . }}
        {ABOLISHED_CLAUSE}
    }}
    """
    return collect_positions(context, client, query)


def discover_candidates(context: Context, client: WikidataClient) -> set[str]:
    """Enumerate candidate position QIDs. Positions already categorised as PEP
    in the review database are always included, so a failing discovery query
    can delay new positions but never drop known ones. Positions reviewed as
    non-PEP are excluded so classification can skip them entirely."""
    candidates: set[str] = set()
    blocked: set[str] = set()
    for qid, is_pep in categorised_position_qids(context):
        if is_pep:
            candidates.add(qid)
        else:
            blocked.add(qid)
    context.log.info(
        "Loaded position verdicts from the review database",
        accepted=len(candidates),
        blocked=len(blocked),
    )
    try:
        candidates.update(query_igo_positions(context, client))
    except Exception as exc:  # noqa: BLE001
        context.log.warning(
            "International-body position discovery failed", error=str(exc)
        )
    for territory in all_territories():
        context.log.info(f"Crawling territory: {territory.qid} ({territory.name})")
        try:
            candidates.update(query_usage_positions(context, client, territory))
            if territory.is_country:
                candidates.update(
                    query_occupation_positions(context, client, territory)
                )
        except Exception as exc:  # noqa: BLE001
            context.log.warning(
                "Position discovery query failed",
                territory=territory.qid,
                name=territory.name,
                error=str(exc),
            )
        context.flush()
    candidates.difference_update(blocked)
    return candidates


@lru_cache(maxsize=POSITION_CACHE_SIZE)
def get_position(context: Context, client: WikidataClient, qid: str) -> Entity | None:
    """Retain evaluated FtM positions across every phase of a PEP crawl."""
    item = client.fetch_item(qid)
    if item is None:
        return None
    return wikidata_position(context, client, item)


def fetch_position_holders(
    context: Context, client: WikidataClient, item: Item
) -> dict[str, datetime | None]:
    """Holders of one position, tolerating a failed holder query.

    Holder queries return multi-megabyte result sets for large legislatures, and
    a transfer that gets cut off mid-body leaves the query service response
    unparseable. The client drops such a response from its cache, so a second
    attempt is a genuinely fresh fetch. If that fails too, this position goes
    without holders for the run — as with the discovery queries above, a single
    bad query must not discard the whole crawl."""
    error: Exception | None = None
    for attempt in range(HOLDER_QUERY_ATTEMPTS):
        try:
            return position_holders(client, item)
        except Exception as exc:  # noqa: BLE001
            error = exc
            context.log.debug(
                "Position holder query failed, retrying",
                position=item.id,
                attempt=attempt + 1,
                error=str(exc),
            )
    context.log.warning(
        "Position holder query failed", position=item.id, error=str(error)
    )
    return {}


def crawl_person(
    context: Context,
    client: WikidataClient,
    accepted: set[str],
    aliases: dict[str, str],
    person_qid: str,
    modified_at: datetime | None,
) -> set[str]:
    """Emit a person and one occupancy for every P39 claim that points to an
    accepted position — not just the position that discovered them. Returns the
    QIDs of the positions this person occupies; the Position entities themselves
    are emitted at the end of the run, once it's known they have holders."""
    occupied_positions: set[str] = set()
    if not is_qid(person_qid):
        return occupied_positions
    item = client.fetch_item(person_qid, modified_at=modified_at)
    if item is None:
        return occupied_positions
    if item.id != person_qid:
        context.resolver.rename_node(person_qid, item.id)
        context.flush()
    entity = wikidata_basic_human(context, client, item)
    if entity is None:
        return occupied_positions

    for claim in item.claims:
        if claim.property != "P39" or claim.qid is None:
            continue
        position_qid = aliases.get(claim.qid, claim.qid)
        if position_qid not in accepted:
            continue
        position = get_position(context, client, position_qid)
        if position is None:
            continue
        occupancy = wikidata_occupancy(context, entity, position, claim)
        if occupancy is not None:
            context.emit(occupancy)
            occupied_positions.add(position_qid)

    if len(occupied_positions) > 0:
        context.emit(entity)
    return occupied_positions


def crawl(context: Context) -> None:
    client = create_wikidata_client(context)

    candidates = discover_candidates(context, client)
    context.log.info(f"Discovered {len(candidates)} candidate positions")

    # Classification must complete over all territories before any person is
    # processed, so that occupancies are built against the final accepted set.
    accepted: set[str] = set()
    aliases: dict[str, str] = {}
    for idx, qid in enumerate(sorted(candidates)):
        item = client.fetch_item(qid)
        if item is None:
            continue
        if item.id != qid:
            context.resolver.rename_node(qid, item.id)
            aliases[qid] = item.id
        position = get_position(context, client, item.id)
        if position is not None:
            accepted.add(item.id)
        if idx % 500 == 0:
            context.log.info(f"Classified {idx} of {len(candidates)} candidates...")
            context.flush()
    context.flush()
    context.log.info(f"Accepted {len(accepted)} of {len(candidates)} positions")

    # One pass over the holders of every accepted position. Each person is
    # fetched and emitted exactly once, with occupancies for all their accepted
    # positions; only QID-keyed bookkeeping is retained across the run.
    done_persons: set[str] = set()
    has_holders: set[str] = set()
    for idx, position_qid in enumerate(sorted(accepted)):
        position_item = client.fetch_item(position_qid)
        if position_item is None:
            continue
        position = get_position(context, client, position_qid)
        if position is None:
            continue
        context.log.info(f"Position [{position.id}]: {position.caption}")
        holders = fetch_position_holders(context, client, position_item)
        for person_qid, modified_at in holders.items():
            if person_qid in done_persons:
                continue
            done_persons.add(person_qid)
            occupied_positions = crawl_person(
                context, client, accepted, aliases, person_qid, modified_at
            )
            has_holders.update(occupied_positions)
            if len(done_persons) % 1000 == 0:
                context.log.info(f"Crawled {len(done_persons)} persons...")
                context.flush()
        if idx % 100 == 0 and idx > 0:
            context.log.info(f"Crawled holders for {idx} positions...")
        context.flush()

    for position_qid in sorted(has_holders):
        position = get_position(context, client, position_qid)
        if position is not None:
            context.emit(position)
    context.flush()
    context.log.info(
        f"Emitted {len(has_holders)} positions and {len(done_persons)} candidate persons"
    )
    cache_info = get_position.cache_info()
    context.log.info(
        "Position entity cache",
        hits=cache_info.hits,
        misses=cache_info.misses,
        evictions=max(0, cache_info.misses - cache_info.currsize),
        size=cache_info.currsize,
        maxsize=cache_info.maxsize,
    )
