import os
from urllib.parse import urljoin

from lxml import etree
from rigour.urls import build_url

from zavod import Context, Entity
from zavod import helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise
from zavod.util import Element

ACCESS_TOKEN = os.environ.get("OPENSANCTIONS_HU_NATIONAL_ASSEMBLY_API_KEY")

# A member's mandates, one per election they won. The parliamentary group
# memberships alongside them cover the same periods, so the parties are read from
# there rather than from the single current group in the list endpoint.
ELECTION_PATH = "./valasztasok/valasztas"
GROUP_PATH = "./kepvcsop-tagsagok/tagsag"
# Roles held outside parliament, e.g. 'Építési és Beruházási Minisztérium
# államtitkára'. Committee seats and parliamentary group offices are also in the
# profile, under bizottsagi-tagsagok and kepvcsop-tisztsegek, but aren't emitted.
FUNCTION_PATH = "./tisztsegek/tisztseg"


def fetch_xml(context: Context, endpoint: str, params: dict[str, str] = {}) -> Element:
    """Fetch and parse one endpoint of the parliament's XML Web API."""
    # Without a token the API answers with an empty body rather than an error.
    assert ACCESS_TOKEN is not None
    url = build_url(
        urljoin(context.data_url, f"{endpoint}.cgi"),
        {
            **params,
            "access_token": ACCESS_TOKEN,
        },
    )
    _, _, _, text = zyte_api.fetch_text(
        context,
        url,
        geolocation="HU",
        cache_days=1,
    )
    return etree.fromstring(text.encode())


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
            topics=["gov.national"],
            lang="hun",
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
