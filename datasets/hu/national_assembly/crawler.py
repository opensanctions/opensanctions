import os
from time import sleep
from urllib.parse import urljoin

from lxml import etree
from rigour.urls import build_url

from zavod import Context, Entity
from zavod import helpers as h
from zavod.extract import zyte_api
from zavod.extract.zyte_api import ZyteAPIRequest
from zavod.stateful.positions import categorise
from zavod.util import Element

ACCESS_TOKEN = os.environ.get("OPENSANCTIONS_HU_NATIONAL_ASSEMBLY_API_KEY")

CACHE_DAYS = 1
# parlament.hu is fronted by a WAF which intermittently answers a request with an
# HTML challenge page instead of the XML document, so a response that doesn't parse
# is retried rather than treated as a source change.
FETCH_ATTEMPTS = 3
FETCH_BACKOFF = 6  # seconds, doubled per attempt
# Length of the response quoted when giving up, enough to tell a challenge page from
# an error page without filling issues.log with markup.
SNIPPET_LEN = 200

# A member's mandates, one per election they won. The parliamentary group
# memberships alongside them cover the same periods, so the parties are read from
# there rather than from the single current group in the list endpoint.
ELECTION_PATH = "./valasztasok/valasztas"
GROUP_PATH = "./kepvcsop-tagsagok/tagsag"
# Offices held besides the mandate, e.g. 'az Országgyűlés elnöke' (Speaker of the
# National Assembly). Committee seats and parliamentary group offices are also in the
# profile, under bizottsagi-tagsagok and kepvcsop-tisztsegek, but aren't emitted.
FUNCTION_PATH = "./tisztsegek/tisztseg"


def fetch_xml(context: Context, endpoint: str, params: dict[str, str] = {}) -> Element:
    """Fetch and parse one endpoint of the parliament's XML Web API.

    Only a response which parses as XML is written to the cache, so a challenge page
    isn't replayed from the cache for the rest of the day.
    """
    # Without a token the API answers with an empty body rather than an error.
    assert ACCESS_TOKEN is not None
    url = build_url(
        urljoin(context.data_url, f"{endpoint}.cgi"),
        {
            **params,
            "access_token": ACCESS_TOKEN,
        },
    )
    request = ZyteAPIRequest(url=url, geolocation="HU")
    for attempt in range(FETCH_ATTEMPTS):
        result = zyte_api.fetch(context, request, cache_days=CACHE_DAYS)
        doc: Element | None = None
        error: str | None = None
        # A response served from the cache reports no status code.
        if result.status_code not in (200, None):
            error = f"HTTP status {result.status_code}"
        else:
            try:
                doc = etree.fromstring(result.response_text.encode())
            except etree.XMLSyntaxError as exc:
                error = str(exc)

        if doc is not None:
            if not result.from_cache:
                context.cache.set(result.cache_fingerprint, result.response_text)
            return doc

        # fetch() reads the cache but never writes it, so this only clears an entry
        # left behind by an earlier run.
        result.invalidate_cache(context)
        # The URL carries the access token, so it's kept out of the logs.
        context.log.warning(
            "No XML in API response",
            endpoint=endpoint,
            params=params,
            status_code=result.status_code,
            media_type=result.media_type,
            from_cache=result.from_cache,
            error=error,
            snippet=result.response_text[:SNIPPET_LEN],
        )
        if attempt + 1 < FETCH_ATTEMPTS:
            sleep(FETCH_BACKOFF * 2**attempt)

    raise RuntimeError(
        f"No XML from {endpoint}.cgi for {params!r} after {FETCH_ATTEMPTS} attempts"
    )


def emit_occupancy(
    context: Context,
    person: Entity,
    position: Entity,
    start_date: str | None,
    end_date: str | None,
    is_pep: bool | None,
    election_date: str | None = None,
    constituency: str | None = None,
    political_group: list[str] = [],
) -> None:
    categorisation = categorise(context, position, default_is_pep=is_pep)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        person=person,
        position=position,
        start_date=start_date,
        end_date=end_date,
        election_date=election_date,
        categorisation=categorisation,
    )
    if occupancy is not None:
        occupancy.add("constituency", constituency)
        # The faction held during this term, as distinct from the person's overall
        # party affiliations.
        occupancy.add("politicalGroup", political_group)
        context.emit(occupancy)
        context.emit(position)
        context.emit(person)


def crawl_member(context: Context, azon: str) -> None:
    doc = fetch_xml(context, "kepviselo", {"p_azon": azon})

    # Hungarian name order, family name first, often carrying a 'Dr.' prefix.
    raw_name = h.xpath_string(doc, "./nev/text()")
    name = h.strip_name_titles(context, raw_name)

    person = context.make("Person")
    person.id = context.make_slug(azon)
    person.add("name", name, original_value=raw_name if name != raw_name else None)
    person.add("citizenship", "hu")
    person.add("website", h.xpath_strings(doc, "./honlap/text()"))
    person.add("political", h.xpath_strings(doc, f"{GROUP_PATH}/@kepvcsop"))

    # Both mandates and group memberships are labelled with the term they fall in,
    # e.g. '2022-2026', which is what ties a faction to a specific mandate.
    groups: dict[str, list[str]] = {}
    for membership in h.xpath_elements(doc, GROUP_PATH):
        term = membership.get("ciklus")
        if term:
            groups.setdefault(term, []).append(h.xpath_string(membership, "@kepvcsop"))

    position = h.make_position(
        context,
        name="Member of the National Assembly of Hungary",
        wikidata_id="Q17590876",
        country="hu",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    for election in h.xpath_elements(doc, ELECTION_PATH):
        emit_occupancy(
            context,
            person=person,
            position=position,
            start_date=h.xpath_string(election, "@mandatum_kezdete"),
            # An ongoing period is expressed as an empty end date.
            end_date=election.get("mandatum_vege") or None,
            is_pep=True,
            election_date=election.get("megvalasztas_napja"),
            constituency=election.get("valasztokerulet"),
            political_group=groups.get(election.get("ciklus") or "", []),
        )

    for function in h.xpath_elements(doc, FUNCTION_PATH):
        function_position = h.make_position(
            context,
            name=h.xpath_string(function, "@megnevezes"),
            country="hu",
            # No topics: these names come straight from the source, so their scope and
            # role are for the review and classification system to decide.
            lang="hun",
            translate_name=True,
        )
        emit_occupancy(
            context,
            person=person,
            position=function_position,
            start_date=h.xpath_string(function, "@tol_datum"),
            end_date=function.get("ig_datum") or None,
            # Not every one of these is a PEP position, so leave it to categorisation.
            is_pep=None,
        )


def crawl(context: Context) -> None:
    doc = fetch_xml(context, "kepviselok")
    for azon in h.xpath_strings(doc, "./kepviselo/@p_azon"):
        crawl_member(context, azon)
