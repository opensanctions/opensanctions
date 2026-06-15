import re
from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The two chamber listing pages expose a second, also-unauthenticated feed whose
# `positions` arrays carry the ministerial portfolios the member-search API lacks.
CONTACT_URLS = [
    "https://contact-details-api.parliament.sa.gov.au/api/HAMembersDetails",
    "https://contact-details-api.parliament.sa.gov.au/api/LCMembersDetails",
]
# The member-search UI links each member to their profile by this opaque pmId.
PROFILE_URL = "https://www.parliament.sa.gov.au/Search/Member?type=member&id=%s"
BR_RE = re.compile(r"<br\s*/?>")


def fetch_portfolios(context: Context) -> dict[int, list[str]]:
    """Map each member's pmId to their ministerial / portfolio role labels.

    These come from the contact-details feeds rather than the member-search API,
    which only reports a single generic role (e.g. "Minister") per member. The
    source strings are dirty: a single entry can pack two roles via an embedded
    ``<br/>``, and values carry stray whitespace and duplicates.
    """
    portfolios: dict[int, list[str]] = {}
    for url in CONTACT_URLS:
        payload = context.fetch_json(url, cache_days=1)
        for member in payload["memberContacts"]:
            roles: list[str] = []
            for raw in member.get("positions", []):
                for part in BR_RE.split(raw):
                    part = part.strip()
                    if part:
                        roles.append(part)
            if roles:
                portfolios[member["pmId"]] = roles
    return portfolios


def crawl_member(
    context: Context,
    positions: dict[str, tuple[Entity, PositionCategorisation]],
    portfolios: dict[int, list[str]],
    row: dict[str, Any],
) -> None:
    house = (row.pop("houseName") or "").strip()
    if house not in positions:
        context.log.warning("Unknown house", house=house)
        return
    position, categorisation = positions[house]

    pm_id = row.pop("pm_Id")
    first = (row.pop("pm_FirstName") or "").strip()
    other = (row.pop("pm_OtherNames") or "").strip()
    last = (row.pop("pm_LastName") or "").strip()

    person = context.make("Person")
    person.id = context.make_slug("member", str(pm_id))
    h.apply_name(
        person,
        first_name=first,
        middle_name=other or None,
        last_name=last,
        lang="eng",
    )
    person.add("title", (row.pop("pm_Title") or "").strip())
    dob = row.pop("pm_DateOfBirth")
    if dob:
        # ISO timestamp at midnight; keep day precision only.
        h.apply_date(person, "birthDate", dob.split("T")[0])
    # The "Represented multiple parties" sentinel is nulled via a type.string lookup.
    person.add("political", row.pop("pp_name"))
    person.add("sourceUrl", PROFILE_URL % pm_id)
    # Constitution Act 1934 (SA) s 31 (Assembly) / s 17 (Council), each subsection
    # (1)(ab): a member's seat is vacated if the member "is not or ceases to be an
    # Australian citizen".
    # https://www.legislation.sa.gov.au/lz?path=%2Fc%2Fa%2Fconstitution+act+1934
    person.add("citizenship", "au")

    # Leadership roles go in Person:position (entity.add dedupes). Portfolios come
    # from the contact-details feeds; pm_Position adds the presiding/whip/leader
    # roles those feeds omit, skipping the generic "Member"/"Minister" labels.
    person.add("position", portfolios.get(pm_id))
    pm_position = (row.pop("pm_Position") or "").strip()
    if pm_position not in ("", "Member", "Minister"):
        person.add("position", pm_position)

    elected = row.pop("mb_ElectedDate")
    start_date: str | None = None
    if elected:
        # e.g. "Mar 21 2026 12:00AM" / "May  4 2021 12:00AM" — drop the midnight
        # time component; .split() also collapses the double space on single digits.
        parts = elected.split()
        if len(parts) >= 3:
            start_date = " ".join(parts[:3])
        else:
            context.log.warning("Unparsable elected date", value=elected)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        start_date=start_date,
    )
    if occupancy is None:
        return
    electorate = (row.pop("electorate") or "").strip()
    # House of Assembly only; Legislative Council members are elected statewide and
    # carry the literal "Unknown" here.
    if electorate and electorate != "Unknown":
        occupancy.add("constituency", electorate)
    context.audit_data(
        row,
        ignore=[
            "ho_name",
            "pm_PositionDesc",
            "pm_ArchiveDate",
            "pm_Deceased",
            "dateTo",
        ],
    )
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    house_assembly = h.make_position(
        context,
        name="Member of the South Australian House of Assembly",
        country="au",
        subnational_area="South Australia",
        wikidata_id="Q18220900",
        topics=["gov.state", "gov.legislative"],
    )
    legislative_council = h.make_position(
        context,
        name="Member of the South Australian Legislative Council",
        country="au",
        subnational_area="South Australia",
        wikidata_id="Q18662245",
        topics=["gov.state", "gov.legislative"],
    )
    context.emit(house_assembly)
    context.emit(legislative_council)
    # Keyed by the API's `houseName` so members map straight to their chamber.
    positions: dict[str, tuple[Entity, PositionCategorisation]] = {
        "House of Assembly": (
            house_assembly,
            categorise(context, house_assembly, default_is_pep=True),
        ),
        "Legislative Council": (
            legislative_council,
            categorise(context, legislative_council, default_is_pep=True),
        ),
    }

    portfolios = fetch_portfolios(context)
    data = context.fetch_json(
        context.data_url,
        method="POST",
        data=b'{"memberType":"current"}',
        headers={"Content-Type": "application/json"},
        cache_days=1,
    )
    if not isinstance(data, list) or not (55 <= len(data) <= 105):
        context.log.warning("Unexpected member count", count=len(data))
    for row in data:
        crawl_member(context, positions, portfolios, row)
