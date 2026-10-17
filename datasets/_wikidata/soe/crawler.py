from collections.abc import Generator, Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from nomenklatura.wikidata import Claim, Item, WikidataClient
from nomenklatura.wikidata.value import clean_wikidata_name
from rigour.ids.wikidata import is_qid
from rigour.territories import get_territories
from rigour.territories.territory import Territory
from rigour.time import iso_datetime
from zavod.shed.wikidata.client import WIKIDATA_QUERY_CACHE, create_wikidata_client
from zavod.shed.wikidata.country import (
    abolished_clause,
    is_abolished,
    item_countries,
)
from zavod.shed.wikidata.human import fetch_human
from zavod.shed.wikidata.position import wikidata_occupancy
from zavod.stateful.positions import categorise

from zavod import Context, Entity
from zavod import helpers as h

ROLE_QUALIFIER = "P2868"
SUBSIDIARY_CHUNK = 100


@dataclass(frozen=True)
class Role:
    key: str
    property: str
    qualifier: str | None
    label: str


@dataclass
class Enterprise:
    """An enterprise item found by discovery, with the evidence each lane
    contributed."""

    qid: str
    modified_at: datetime | None = None
    marked: bool = False
    owners: list[Territory] = field(default_factory=list)
    parents: set[str] = field(default_factory=set)


def load_roles(context: Context) -> list[Role]:
    roles: list[Role] = []
    for spec in context.dataset.config["roles"]:
        role = Role(
            key=spec.pop("key"),
            property=spec.pop("property"),
            qualifier=spec.pop("qualifier", None),
            label=spec.pop("label"),
        )
        if len(spec) > 0:
            raise ValueError(f"Unknown keys in role {role.key!r}: {list(spec)}")
        if not role.property.startswith("P") or not role.property[1:].isdigit():
            raise ValueError(f"Invalid property in role {role.key!r}")
        if role.qualifier is not None and not is_qid(role.qualifier):
            raise ValueError(f"Invalid qualifier in role {role.key!r}")
        roles.append(role)
    return roles


def load_soe_types(context: Context) -> set[str]:
    types: dict[str, str] = context.dataset.config["soe_types"]
    for qid in types:
        if not is_qid(qid):
            raise ValueError(f"Invalid QID in soe_types: {qid!r}")
    return set(types)


def match_role(roles: list[Role], claim: Claim) -> Role | None:
    """Find the first role row for a seat claim. A row without a qualifier only
    matches claims that carry no role qualifier, so seats with an unlisted role
    are skipped rather than read as plain board seats."""
    qualifiers = {q.qid for q in claim.get_qualifier(ROLE_QUALIFIER)}
    for role in roles:
        if role.property != claim.property:
            continue
        if role.qualifier is None and len(qualifiers) == 0:
            return role
        if role.qualifier is not None and role.qualifier in qualifiers:
            return role
    return None


def all_territories() -> Generator[Territory, None, None]:
    """Territories that can own an enterprise: countries and subnational
    jurisdictions that map onto an FtM country code."""
    for territory in get_territories():
        if territory.is_historical:
            continue
        if territory.ftm_country is None:
            continue
        yield territory


def query_enterprises(
    context: Context, client: WikidataClient, query: str
) -> Generator[tuple[str, Any], None, None]:
    """Run a discovery query and yield (enterprise QID, result row) for each
    enterprise that is not dissolved before the cutoff."""
    response = client.query(query, cache_days=WIKIDATA_QUERY_CACHE)
    for bind in response.results:
        qid = bind.plain("enterprise")
        if qid is None or not is_qid(qid):
            continue
        if is_abolished(bind.plain("abolished")):
            context.log.debug(f"Skipping dissolved enterprise: {qid}")
            continue
        yield qid, bind


def get_enterprise(
    enterprises: dict[str, Enterprise], qid: str, bind: Any
) -> Enterprise:
    enterprise = enterprises.get(qid)
    if enterprise is None:
        enterprise = Enterprise(qid=qid)
        enterprises[qid] = enterprise
    modified = bind.plain("modified")
    if modified is not None:
        enterprise.modified_at = iso_datetime(modified)
    return enterprise


def discover(
    context: Context,
    client: WikidataClient,
    roles: list[Role],
    soe_types: set[str],
) -> dict[str, Enterprise]:
    seat_path = "|".join(sorted({f"wdt:{r.property}" for r in roles}))
    seat_filter = f"FILTER EXISTS {{ ?enterprise {seat_path} ?seat . }}"
    abolished = abolished_clause("enterprise")
    enterprises: dict[str, Enterprise] = {}

    # Lane A: listed type or legal form, one query per type.
    for type_qid in sorted(soe_types):
        query = f"""
        SELECT DISTINCT ?enterprise ?modified ?abolished WHERE {{
            ?enterprise wdt:P31|wdt:P1454 wd:{type_qid} .
            {seat_filter}
            ?enterprise schema:dateModified ?modified .
            {abolished}
        }}
        """
        for qid, bind in query_enterprises(context, client, query):
            get_enterprise(enterprises, qid, bind).marked = True
    lane_a = len(enterprises)
    context.log.info("Lane A: listed types", enterprises=lane_a)

    # Lane B: owned directly by a territory, one query per territory QID.
    lane_b: set[str] = set()
    for territory in all_territories():
        for territory_qid in sorted(territory.qids):
            query = f"""
            SELECT DISTINCT ?enterprise ?modified ?abolished WHERE {{
                ?enterprise p:P127 ?owner .
                ?owner ps:P127 wd:{territory_qid} .
                FILTER NOT EXISTS {{ ?owner wikibase:rank wikibase:DeprecatedRank }}
                {seat_filter}
                ?enterprise schema:dateModified ?modified .
                {abolished}
            }}
            """
            try:
                for qid, bind in query_enterprises(context, client, query):
                    enterprise = get_enterprise(enterprises, qid, bind)
                    if territory not in enterprise.owners:
                        enterprise.owners.append(territory)
                    lane_b.add(qid)
            except Exception as exc:  # noqa: BLE001
                context.log.warning(
                    "Territory ownership query failed",
                    territory=territory_qid,
                    name=territory.name,
                    error=str(exc),
                )
        context.flush()
    context.log.info(
        "Lane B: owned by a territory",
        enterprises=len(lane_b),
        new=len(enterprises) - lane_a,
    )

    # Lane C: one hop of subsidiaries below lanes A and B.
    parents = sorted(enterprises)
    before_c = len(enterprises)
    lane_c: set[str] = set()
    for idx in range(0, len(parents), SUBSIDIARY_CHUNK):
        chunk = parents[idx : idx + SUBSIDIARY_CHUNK]
        values = " ".join(f"wd:{qid}" for qid in chunk)
        query = f"""
        SELECT DISTINCT ?enterprise ?parent ?modified ?abolished WHERE {{
            VALUES ?parent {{ {values} }}
            ?enterprise wdt:P127|wdt:P749 ?parent .
            {seat_filter}
            ?enterprise schema:dateModified ?modified .
            {abolished}
        }}
        """
        for qid, bind in query_enterprises(context, client, query):
            parent = bind.plain("parent")
            if parent is None or parent == qid:
                continue
            get_enterprise(enterprises, qid, bind).parents.add(parent)
            lane_c.add(qid)
    context.log.info(
        "Lane C: subsidiaries",
        enterprises=len(lane_c),
        new=len(enterprises) - before_c,
    )
    return enterprises


def enterprise_countries(
    client: WikidataClient,
    enterprises: dict[str, Enterprise],
    enterprise: Enterprise,
) -> set[str]:
    """The enterprise's own country, else its owning territories', else its
    parents'."""
    countries = {c.text for c in item_countries(client, enterprise.qid)}
    if len(countries) == 0:
        countries = {t.ftm_country for t in enterprise.owners}
    if len(countries) == 0:
        for parent in enterprise.parents:
            countries.update(c.text for c in item_countries(client, parent))
    return {c for c in countries if c is not None}


def is_marked(enterprises: dict[str, Enterprise], enterprise: Enterprise) -> bool:
    if enterprise.marked:
        return True
    for parent in enterprise.parents:
        if enterprises[parent].marked:
            return True
    return False


def make_seat_position(
    context: Context,
    item: Item,
    role: Role,
    label: str,
    countries: set[str],
    topics: list[str],
    subnational_areas: Iterable[str],
) -> Entity | None:
    position = h.make_position(
        context,
        name=f"{role.label}, {label}",
        country=sorted(countries),
        topics=topics,
    )
    # The default id is derived from the name, which changes with the label:
    position.id = context.make_id(item.id, role.key)
    position.add("subnationalArea", subnational_areas)
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return None
    position.set("topics", [t for t in categorisation.topics if t != "role.pep"])
    return position


Seats = dict[str, list[tuple[Entity, Claim]]]


def read_seats(
    context: Context,
    client: WikidataClient,
    roles: list[Role],
    enterprises: dict[str, Enterprise],
) -> Seats:
    seats: Seats = {}
    seat_count = 0
    done: set[str] = set()
    for idx, enterprise in enumerate(sorted(enterprises.values(), key=lambda e: e.qid)):
        if idx > 0 and idx % 500 == 0:
            context.log.info(f"Read seats of {idx} of {len(enterprises)} enterprises")
            context.flush()
        item = client.fetch_item(enterprise.qid, modified_at=enterprise.modified_at)
        if item is None or item.id in done:
            continue
        done.add(item.id)
        if item.label is None or item.label.text is None:
            context.log.info("Enterprise has no label", qid=item.id)
            continue
        label = clean_wikidata_name(item.label.text)
        if label is None:
            continue
        countries = enterprise_countries(client, enterprises, enterprise)
        if len(countries) == 0:
            context.log.info("Enterprise has no country", qid=item.id, label=label)
            continue
        topics = ["gov.soe" if is_marked(enterprises, enterprise) else "gov.admin"]
        subnational = [t.name for t in enterprise.owners if not t.is_country]
        if len(subnational) > 0:
            topics.append("gov.state")

        positions: dict[str, Entity | None] = {}
        for claim in item.claims:
            if claim.qid is None:
                continue
            role = match_role(roles, claim)
            if role is None:
                continue
            if role.key not in positions:
                positions[role.key] = make_seat_position(
                    context, item, role, label, countries, topics, subnational
                )
            position = positions[role.key]
            if position is None:
                continue
            seats.setdefault(claim.qid, []).append((position, claim))
            seat_count += 1
    context.log.info("Read seats", seats=seat_count, persons=len(seats))
    return seats


def crawl(context: Context) -> None:
    client = create_wikidata_client(context)
    roles = load_roles(context)
    soe_types = load_soe_types(context)

    enterprises = discover(context, client, roles, soe_types)
    context.log.info(f"Discovered {len(enterprises)} enterprises")
    seats = read_seats(context, client, roles, enterprises)

    emitted_positions: set[str] = set()
    persons = 0
    for idx, person_qid in enumerate(sorted(seats)):
        if idx > 0 and idx % 1000 == 0:
            context.log.info(f"Crawled {idx} of {len(seats)} persons")
            context.flush()
        fetched = fetch_human(context, client, person_qid)
        if fetched is None:
            continue
        _, person = fetched
        has_occupancy = False
        for position, claim in seats[person_qid]:
            occupancy = wikidata_occupancy(context, person, position, claim)
            if occupancy is None:
                continue
            context.emit(occupancy)
            has_occupancy = True
            if position.id not in emitted_positions:
                assert position.id is not None
                emitted_positions.add(position.id)
                context.emit(position)
        if has_occupancy:
            context.emit(person)
            persons += 1
    context.log.info(
        "Emitted SOE leadership",
        positions=len(emitted_positions),
        persons=persons,
    )
