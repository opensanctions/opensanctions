import time
from dataclasses import dataclass, field
from typing import Any

from banal import ensure_list
from requests.exceptions import HTTPError
from rigour.urls import build_url

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.runtime.http_ import request_hash
from zavod.stateful.positions import PositionCategorisation, categorise

# One term roster fits in a single response; this bounds it and flags overflow.
ROSTER_LIMIT = 10000

# Fields each record type carries that the crawler deliberately leaves unmapped.
# Every object the API returns has `id` (the JSON-LD node IRI) and `type` (the
# JSON-LD @type tag).

# A PeriodOfTime, as carried by `temporal` and `memberDuring`.
PERIOD_IGNORE = ["id", "type"]

# A parliamentary term, i.e. the `org/ep-{n}` corporate body.
TERM_IGNORE = [
    "id",  # JSON-LD node IRI
    "type",  # JSON-LD @type tag
    "identifier",  # term number, already the loop index
    "label",  # term number as a label
    "altLabel",  # always "European Parliament"
    "prefLabel",  # always "European Parliament"
    "classification",  # always EU_INSTITUTION
    "represents",  # not set on the institution body
    "source",  # EP provenance flag
    "linkedTo",  # refs to related bodies
    "isVersionOf",  # parent body ref
    "notation_codictBodyId",  # internal EP id
    "notation_providerTemporalBodyId",  # internal EP id
]

# A political group or national party, i.e. an `org/{id}` corporate body.
ORG_IGNORE = [
    "id",  # JSON-LD node IRI
    "type",  # JSON-LD @type tag
    "identifier",  # equals the local_id we already keep
    "classification",  # group/committee type, not needed on the org
    "temporal",  # the body's own date range, unused
    "source",  # EP provenance flag
    "linkedTo",  # refs to related bodies
    "isVersionOf",  # term-invariant parent group ref (e.g. org/PPE)
    "notation_codictBodyId",  # internal EP id
    "notation_providerTemporalBodyId",  # internal EP id
]

# A membership of a political group or national party.
MEMBERSHIP_IGNORE = [
    "id",  # JSON-LD node IRI
    "type",  # JSON-LD @type tag
    "identifier",  # internal membership id
    "notation_codictFunctionId",  # internal EP function id
    "contactPoint",  # office address and phone, not modelled
]

# An MEP.
MEP_IGNORE = [
    "id",  # JSON-LD node IRI
    "type",  # JSON-LD @type tag
    "notation_codictPersonId",  # internal EP id, duplicate of identifier
    "hasEmail",  # contact detail, not screening-relevant
    "hasHonorificPrefix",  # honorific (Mr/Ms)
    "homepage",  # personal website, not modelled
    "account",  # social media accounts, not modelled
    "img",  # portrait photo URL
    "sortLabel",  # sorting key, redundant with the name
    "upperFamilyName",  # uppercase form of familyName
    "upperGivenName",  # uppercase form of givenName
    "upperOfficialFamilyName",  # uppercase form of the native family name
    "upperOfficialGivenName",  # uppercase form of the native given name
]


@dataclass
class Term:
    number: int
    start: str | None
    end: str | None


@dataclass
class OrgInfo:
    local_id: str
    name: str | None
    acronym: str | None
    countries: list[str] = field(default_factory=list)


class LeakyBucketRateLimiter:
    """Leaky-bucket rate limiter: paces calls to `rate` per second by sleeping."""

    def __init__(self, rate: float) -> None:
        self._interval = 1.0 / rate
        self._tat = time.monotonic()

    def acquire(self) -> None:
        now = time.monotonic()
        self._tat = max(self._tat, now)
        if self._tat > now:
            time.sleep(self._tat - now)
        self._tat += self._interval


# The API allows 500 requests per 5 minutes (~1.67/s); 1.5/s keeps a safety margin.
rate_limiter = LeakyBucketRateLimiter(1.5)


def last_segment(value: Any) -> str | None:
    """Return the last path segment of an EU authority URI, e.g.
    `.../country/BEL` -> `BEL`, `.../human-sex/MALE` -> `MALE`."""
    if isinstance(value, list):
        value = value[0] if value else None
    if not isinstance(value, str) or not value:
        return None
    return value.rsplit("/", 1)[-1]


def pick_label(value: Any) -> str | None:
    """Return a label: an API string, or English (else any) from a language-keyed dict."""
    if isinstance(value, str):
        return value
    if not isinstance(value, dict):
        return None
    strings = [v for v in value.values() if isinstance(v, str)]
    return (
        value["en"] if isinstance(value.get("en"), str) else next(iter(strings), None)
    )


def fetch_data(
    context: Context, path: str, cache_days: int = 7, **params: Any
) -> list[Any]:
    """Fetch a JSON-LD endpoint and return its `data` array."""
    params.setdefault("format", "application/ld+json")
    url = f"https://data.europarl.europa.eu/api/v2{path}"
    # Ideally fetch_json would report whether this was a cache hit, or support rate
    # limiting itself; absent that, recreate its cache fingerprint here so we only
    # pace on a real network call.
    fingerprint = request_hash(build_url(url, params))
    if context.cache.get(fingerprint, max_age=cache_days) is None:
        rate_limiter.acquire()
    body = context.fetch_json(url, params=params, cache_days=cache_days)
    return ensure_list(body["data"])


def fetch_terms(context: Context) -> list[Term]:
    """Walk the parliament institution bodies `org/ep-{n}` to learn each term's
    date range. Stop at the first term that does not exist."""
    terms: list[Term] = []
    # 16 is far above any real term number; the loop stops at the first 404.
    for number in range(1, 16):
        try:
            rows = fetch_data(context, f"/corporate-bodies/ep-{number}")
        except HTTPError as err:
            if err.response is not None and err.response.status_code == 404:
                break
            raise
        assert len(rows) == 1, (number, len(rows))
        body = rows[0]
        temporal = body.pop("temporal")
        term_start = temporal.pop("startDate")
        term_end = temporal.pop("endDate", None)
        context.audit_data(temporal, ignore=PERIOD_IGNORE)
        context.audit_data(body, ignore=TERM_IGNORE)
        terms.append(Term(number, term_start, term_end))
    return terms


def fetch_org(context: Context, org_ref: str, cache: dict[str, OrgInfo]) -> OrgInfo:
    """Resolve an `org/{id}` reference to its name, acronym and countries."""
    if org_ref in cache:
        return cache[org_ref]
    local_id = org_ref.split("/", 1)[-1]
    rows = fetch_data(context, f"/corporate-bodies/{local_id}")
    assert len(rows) == 1, (org_ref, len(rows))
    data = rows[0]
    countries = [
        c for c in map(last_segment, ensure_list(data.pop("represents", None))) if c
    ]
    # prefLabel is the body's full name; altLabel is a shorter form, used as fallback.
    # Pop both up front so audit_data does not flag the one the `or` would skip.
    pref_name = pick_label(data.pop("prefLabel"))
    alt_name = pick_label(data.pop("altLabel"))
    name = pref_name or alt_name
    acronym = data.pop("label")
    # The API uses "-" as a placeholder for a missing value.
    name = None if name == "-" else name
    acronym = None if acronym == "-" else acronym
    context.audit_data(data, ignore=ORG_IGNORE)
    info = OrgInfo(local_id=local_id, name=name, acronym=acronym, countries=countries)
    cache[org_ref] = info
    return info


def crawl_group_membership(
    context: Context,
    person: Entity,
    membership: dict[str, Any],
    is_eu_group: bool,
    cache: dict[str, OrgInfo],
) -> None:
    """Emit the political group or national party and the person's membership in it."""
    info = fetch_org(context, membership.pop("organization"), cache)

    org = context.make("Organization")
    # The API uses "-" where it records no party. A nameless organization, and
    # a membership pointing at one, carry no information.
    if info.name is None and info.acronym is None:
        return
    # The API models a group as a distinct body per term. Key by name so the
    # same party or group is one entity across terms, not one per term.
    org.id = context.make_slug(
        "eu-group" if is_eu_group else "nat-party", info.name or info.local_id
    )
    org.add("name", info.name)
    org.add("name", info.acronym)
    if is_eu_group:
        org.add("country", "eu")
    else:
        org.add("country", info.countries)
    context.emit(org)

    # memberDuring carries the start and end dates of the membership period.
    period = membership.pop("memberDuring")
    entity = context.make("Membership")
    entity.id = context.make_id(
        person.id, org.id, period.get("startDate"), period.get("endDate")
    )
    entity.add("member", person)
    entity.add("organization", org)
    role = last_segment(membership.pop("role"))
    if role is not None:
        entity.add("role", role.replace("_", " ").lower())
    h.apply_date(entity, "startDate", period.pop("startDate"))
    h.apply_date(entity, "endDate", period.pop("endDate", None))
    context.audit_data(period, ignore=PERIOD_IGNORE)
    context.audit_data(membership, ignore=MEMBERSHIP_IGNORE)
    context.emit(entity)


def crawl_mep(
    context: Context,
    mep_id: str,
    position: Entity,
    categorisation: PositionCategorisation,
    cache: dict[str, OrgInfo],
) -> None:
    """Fetch one MEP and emit the person, their mandates and group memberships."""
    rows = fetch_data(context, f"/meps/{mep_id}")
    if not rows:
        context.log.warning("No data for MEP", mep_id=mep_id)
        return
    data = rows[0]

    person = context.make("Person")
    person.id = context.make_slug(data.pop("identifier"))
    # Names are plain strings, but gender and citizenship are EU authority URIs.
    person.add("name", pick_label(data.pop("label")))
    person.add("firstName", data.pop("givenName", None))
    person.add("lastName", data.pop("familyName", None))
    # Cyrillic- and Greek-name MEPs also carry their name in the native script.
    h.apply_name(
        person,
        given_name=data.pop("officialGivenName", None),
        last_name=data.pop("officialFamilyName", None),
    )
    person.add("gender", last_segment(data.pop("hasGender", None)))
    person.add("birthPlace", data.pop("placeOfBirth", None))
    for citizenship in ensure_list(data.pop("citizenship", None)):
        person.add("citizenship", last_segment(citizenship))
    h.apply_date(person, "birthDate", data.pop("bday", None))
    h.apply_date(person, "deathDate", data.pop("deathDate", None))
    person.add("sourceUrl", f"https://www.europarl.europa.eu/meps/en/{mep_id}")
    memberships = ensure_list(data.pop("hasMembership", None))
    context.audit_data(data, ignore=MEP_IGNORE)

    # One occupancy per term: a mandate is an EU_INSTITUTION membership in
    # org/ep-{term}. make_occupancy decides PEP relevance from the end date and
    # drops stale ones, so a person is emitted only if a mandate is still relevant.
    occupancies: list[Entity] = []
    groups: list[tuple[bool, dict[str, Any]]] = []
    for membership in memberships:
        # Consumed here to dispatch; popped so the group handler need not ignore it.
        group = last_segment(membership.pop("membershipClassification", None))
        result = context.lookup("membership_classification", group)
        if result is None:
            context.log.warning(
                "Unknown membership classification", group=group, mep_id=mep_id
            )
            continue
        if result.value == "mandate":
            org_ref = membership.get("organization")
            if not isinstance(org_ref, str) or not org_ref.startswith("org/ep-"):
                context.log.warning(
                    "Mandate is not held in a parliamentary term",
                    organization=org_ref,
                    mep_id=mep_id,
                )
                continue
            period = membership["memberDuring"]
            occupancy = h.make_occupancy(
                context,
                person,
                position,
                start_date=period["startDate"],
                end_date=period.get("endDate"),
                # The source always lists an end date for a past-term mandate, so a
                # missing end date only ever means the current, ongoing term.
                no_end_implies_current=True,
                categorisation=categorisation,
            )
            if occupancy is not None:
                occupancies.append(occupancy)
        elif result.value == "eu-group":
            groups.append((True, membership))
        elif result.value == "nat-party":
            groups.append((False, membership))
    if not occupancies:
        return

    for occupancy in occupancies:
        context.emit(occupancy)
    context.emit(person)
    for is_eu_group, membership in groups:
        crawl_group_membership(context, person, membership, is_eu_group, cache)


def crawl(context: Context) -> None:
    """Crawl MEPs from every parliamentary term within the PEP relevance window."""
    position = h.make_position(
        context,
        "Member of the European Parliament",
        wikidata_id="Q27169",
        country="eu",
        topics=["gov.igo", "gov.legislative"],
        lang="eng",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    # The position may have been un-flagged as a PEP position in the review UI.
    if not categorisation.is_pep:
        return
    context.emit(position)

    # Keep terms that are ongoing or ended within the PEP relevance window.
    cutoff = h.earliest_term_start(position.get("topics"))
    terms = [t for t in fetch_terms(context) if t.end is None or t.end >= cutoff]
    context.log.info(
        "Crawling MEP terms within PEP relevance window",
        cutoff=cutoff,
        terms=[t.number for t in terms],
    )

    mep_ids: set[str] = set()
    for term in terms:
        rows = fetch_data(
            context,
            "/meps",
            **{"parliamentary-term": term.number, "limit": ROSTER_LIMIT},
        )
        if len(rows) >= ROSTER_LIMIT:
            context.log.warning("Term roster may be truncated", term=term.number)
        for row in rows:
            mep_ids.add(str(row["identifier"]))
    context.log.info("Fetched MEP roster", count=len(mep_ids))

    cache: dict[str, OrgInfo] = {}
    for mep_id in sorted(mep_ids):
        crawl_mep(context, mep_id, position, categorisation, cache)
