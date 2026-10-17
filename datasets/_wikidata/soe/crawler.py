from collections import defaultdict
from collections.abc import Generator, Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from nomenklatura.wikidata import Claim, Item, WikidataClient
from nomenklatura.wikidata.value import clean_wikidata_name
from rigour.ids.wikidata import is_qid
from rigour.territories import get_territory, get_territory_by_qid
from rigour.territories.territory import Territory
from rigour.time import iso_datetime
from zavod.shed.wikidata.client import WIKIDATA_QUERY_CACHE, create_wikidata_client
from zavod.shed.wikidata.country import (
    abolished_clause,
    is_abolished,
    item_countries,
)
from zavod.shed.wikidata.human import fetch_human
from zavod.shed.wikidata.igo import INTL_ORGS, IntlOrg
from zavod.shed.wikidata.position import wikidata_occupancy
from zavod.stateful.positions import categorise

from zavod import Context, Entity
from zavod import helpers as h

ROLE_QUALIFIER = "P2868"


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


def load_types(context: Context, key: str) -> set[str]:
    types: dict[str, str] = context.dataset.config[key]
    for qid in types:
        if not is_qid(qid):
            raise ValueError(f"Invalid QID in {key}: {qid!r}")
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


def state_territory(qid: str | None) -> Territory | None:
    """The territory a QID names, if it is a current one that maps onto an FtM
    country code."""
    if qid is None:
        return None
    territory = get_territory_by_qid(qid)
    if territory is None or territory.is_historical:
        return None
    if territory.ftm_country is None:
        return None
    return territory


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
    government_types: set[str],
) -> dict[str, Enterprise]:
    seat_path = "|".join(sorted({f"wdt:{r.property}" for r in roles}))
    seat_filter = f"FILTER EXISTS {{ ?enterprise {seat_path} ?seat . }}"
    abolished = abolished_clause("enterprise")
    enterprises: dict[str, Enterprise] = {}

    # Lane A: typed (or legally formed) as a state-owned enterprise, anywhere
    # in the class tree. The tree's historical and municipal forms are dropped
    # by `excluded_types` when seats are read.
    query = f"""
    SELECT DISTINCT ?enterprise ?modified ?abolished WHERE {{
        ?enterprise (wdt:P31|wdt:P1454)/wdt:P279* wd:Q270791 .
        {seat_filter}
        ?enterprise schema:dateModified ?modified .
        {abolished}
    }}
    """
    for qid, bind in query_enterprises(context, client, query):
        get_enterprise(enterprises, qid, bind).marked = True
    lane_a = len(enterprises)
    context.log.info("Lane A: state-owned enterprise types", enterprises=lane_a)

    # Lane B: owned by, or subordinate to, a state actor: a territory, a
    # government body placed in one, or a registered international body. The
    # query keeps link targets that could be one (an ISO country or subdivision
    # code, a government type, a registry entry); which ones are is decided
    # here, against rigour. Reading all links instead runs past the query
    # service's time limit.
    types = " ".join(f"wd:{qid}" for qid in sorted(government_types))
    intl_orgs = " ".join(f"wd:{qid}" for qid in sorted(INTL_ORGS))
    query = f"""
    SELECT DISTINCT ?enterprise ?modified ?abolished ?link ?government
        ?jurisdiction ?country WHERE {{
        ?enterprise p:P127|p:P749 ?statement .
        ?statement ps:P127|ps:P749 ?link .
        {{ ?link wdt:P297|wdt:P300 ?code . }}
        UNION
        {{ ?link wdt:P31 ?government . VALUES ?government {{ {types} }} }}
        UNION
        {{ VALUES ?link {{ {intl_orgs} }} }}
        FILTER NOT EXISTS {{ ?statement wikibase:rank wikibase:DeprecatedRank }}
        {seat_filter}
        ?enterprise schema:dateModified ?modified .
        OPTIONAL {{ ?link wdt:P1001 ?jurisdiction . }}
        OPTIONAL {{ ?link wdt:P17 ?country . }}
        {abolished}
    }}
    """
    links: dict[str, set[str]] = defaultdict(set)
    binds: dict[str, Any] = {}
    government: set[str] = set()
    jurisdictions: dict[str, set[str]] = defaultdict(set)
    countries: dict[str, set[str]] = defaultdict(set)
    for qid, bind in query_enterprises(context, client, query):
        link = bind.plain("link")
        if link is None:
            continue
        links[qid].add(link)
        binds[qid] = bind
        if bind.plain("government") is not None:
            government.add(link)
        jurisdiction = bind.plain("jurisdiction")
        if jurisdiction is not None:
            jurisdictions[link].add(jurisdiction)
        country = bind.plain("country")
        if country is not None:
            countries[link].add(country)

    def link_territories(link: str) -> list[Territory]:
        territory = state_territory(link)
        if territory is not None:
            return [territory]
        if link not in government:
            return []
        # A body with a jurisdiction belongs to it alone: an agency of a city
        # stays out even though its country is known.
        places = jurisdictions.get(link) or countries.get(link, set())
        found = (state_territory(place) for place in sorted(places))
        return [t for t in found if t is not None]

    lane_b = 0
    via_government = 0
    via_intl_org = 0
    for qid, enterprise_links in sorted(links.items()):
        owners: list[Territory] = []
        intl_link = any(link in INTL_ORGS for link in enterprise_links)
        for link in sorted(enterprise_links):
            for territory in link_territories(link):
                if territory not in owners:
                    owners.append(territory)
        if len(owners) == 0 and not intl_link:
            continue
        lane_b += 1
        if intl_link:
            via_intl_org += 1
        elif all(state_territory(link) is None for link in enterprise_links):
            via_government += 1
        get_enterprise(enterprises, qid, binds[qid]).owners = owners
    context.log.info(
        "Lane B: linked to a state actor",
        enterprises=lane_b,
        via_government=via_government,
        via_intl_org=via_intl_org,
        new=len(enterprises) - lane_a,
    )

    return enterprises


def enterprise_countries(client: WikidataClient, enterprise: Enterprise) -> set[str]:
    """The enterprise's own current country, else its linked territories'."""
    countries: set[str] = set()
    for text in item_countries(client, enterprise.qid):
        territory = get_territory(text.text) if text.text is not None else None
        if territory is None or territory.is_historical:
            continue
        if territory.ftm_country is not None:
            countries.add(territory.ftm_country)
    if len(countries) == 0:
        for owner in enterprise.owners:
            if owner.ftm_country is not None:
                countries.add(owner.ftm_country)
    return countries


def make_seat_position(
    context: Context,
    item: Item,
    role: Role,
    label: str,
    countries: set[str],
    topics: list[str],
    subnational_areas: Iterable[str],
    default_is_pep: bool | None,
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
    categorisation = categorise(context, position, default_is_pep=default_is_pep)
    if not categorisation.is_pep:
        return None
    position.set("topics", [t for t in categorisation.topics if t != "role.pep"])
    return position


def item_intl_org(item: Item) -> IntlOrg | None:
    """The registered international body an item is, or is part of through a
    P749 parent organization link. An international body that owns a stake
    (P127) is an investor, not a parent."""
    intl_org = INTL_ORGS.get(item.id)
    if intl_org is not None:
        return intl_org
    for claim in item.claims:
        if claim.property == "P749" and claim.qid is not None:
            intl_org = INTL_ORGS.get(claim.qid)
            if intl_org is not None:
                return intl_org
    return None


Seats = dict[str, list[tuple[Entity, Claim]]]


def read_seats(
    context: Context,
    client: WikidataClient,
    roles: list[Role],
    excluded_types: set[str],
    enterprises: dict[str, Enterprise],
) -> Seats:
    seats: Seats = {}
    seat_count = 0
    excluded = 0
    done: set[str] = set()
    for idx, enterprise in enumerate(sorted(enterprises.values(), key=lambda e: e.qid)):
        if idx > 0 and idx % 500 == 0:
            context.log.info(f"Read seats of {idx} of {len(enterprises)} enterprises")
            context.flush()
        item = client.fetch_item(enterprise.qid, modified_at=enterprise.modified_at)
        if item is None or item.id in done:
            continue
        done.add(item.id)
        if not item.types.isdisjoint(excluded_types):
            excluded += 1
            continue
        if item.label is None or item.label.text is None:
            context.log.info("Enterprise has no label", qid=item.id)
            continue
        label = clean_wikidata_name(item.label.text)
        if label is None:
            continue
        intl_org = item_intl_org(item)
        if intl_org is None and not enterprise.marked and len(enterprise.owners) == 0:
            # Found only through an international body's stake in it:
            # an investment, not part of the body.
            continue
        default_is_pep: bool | None = True
        if intl_org is not None:
            # International bodies carry the registry's pseudo-country and
            # topics; those outside `gov.igo` go to review undecided, as their
            # positions do in wd_peps.
            countries = {intl_org.country}
            topics = sorted(intl_org.topics)
            subnational: list[str] = []
            if "gov.igo" not in intl_org.topics:
                default_is_pep = None
        else:
            countries = enterprise_countries(client, enterprise)
            if len(countries) == 0:
                context.log.info("Enterprise has no country", qid=item.id, label=label)
                continue
            topics = ["gov.soe" if enterprise.marked else "gov.admin"]
            # A national owner makes the body national, whichever subdivisions
            # share in it.
            subnational = [t.name for t in enterprise.owners if not t.is_country]
            if any(t.is_country for t in enterprise.owners):
                subnational = []
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
                    context,
                    item,
                    role,
                    label,
                    countries,
                    topics,
                    subnational,
                    default_is_pep,
                )
            position = positions[role.key]
            if position is None:
                continue
            seats.setdefault(claim.qid, []).append((position, claim))
            seat_count += 1
    context.log.info(
        "Read seats", seats=seat_count, persons=len(seats), excluded=excluded
    )
    return seats


def crawl(context: Context) -> None:
    client = create_wikidata_client(context)
    roles = load_roles(context)
    excluded_types = load_types(context, "excluded_types")
    government_types = load_types(context, "government_types")

    enterprises = discover(context, client, roles, government_types)
    context.log.info(f"Discovered {len(enterprises)} enterprises")
    seats = read_seats(context, client, roles, excluded_types, enterprises)

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
