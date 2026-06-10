from collections import defaultdict
from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The Parliamentary Handbook (handbook.aph.gov.au) is backed by this open OData API.
# It covers every parliamentarian since Federation (1901); make_occupancy gates each
# term on PEP duration, so only current and recently-ended terms survive.
API = "https://handbookapi.aph.gov.au/api"
# The API's sentinel date meaning "still in progress" on term end dates.
ONGOING = "1900-01-01"

# Biographical, career-summary and presentation fields we deliberately don't extract.
# MPorSenator and the Represented*/ElectorateService/ServiceHistory_* aggregates are
# career-scope rollups; the per-term service records are the source of truth instead.
INDIVIDUAL_IGNORE = [
    "Age",
    "Age_String",
    "DisplayName",
    "ElectedMemberNo",
    "ElectedSenatorNo",
    "Electorate",
    "ElectorateService",
    "FirstNations",
    "FirstNationsOption",
    "FirstNationsText",
    "Honours",
    "Image",
    "InCurrentParliament",
    "MPorSenator",
    "MaritalStatus",
    "Occupations",
    "ParliamentaryPositions",
    "PartyAbbrev",
    "PartyParliamentaryService",
    "PlaceOfDeath",
    "PortraitNote",
    "Qualifications",
    "RepresentedElectorates",
    "RepresentedMinistries",
    "RepresentedParliaments",
    "RepresentedParties",
    "RepresentedShadowMinistries",
    "RepresentedStates",
    "SecondaryOccupations",
    "SecondarySchool",
    "SenateState",
    "ServiceHistory_Days",
    "ServiceHistory_Duration",
    "ServiceHistory_End",
    "ServiceHistory_Start",
    "State",
    "StateAbbrev",
    "StateOfDeath",
    "StateOrTerritory",
]
SERVICE_IGNORE = [
    "OBY",
    "PHID",
    "DisplayName",
    "ROSTypeID",
    "ValueAbbrev1",
    "Text1",
    "Text2",
    "Text3",
    "Value3",
]


def fetch_collection(
    context: Context, resource: str, query: dict[str, str]
) -> list[dict[str, Any]]:
    params = dict(query)
    params["$count"] = "true"
    data = context.fetch_json(f"{API}/{resource}", params=params, cache_days=1)
    records: list[dict[str, Any]] = data["value"]
    # The API currently returns each collection in a single response. If server-side
    # paging is ever enabled, fail rather than silently crawl a truncated dataset.
    if "@odata.nextLink" in data:
        raise ValueError(f"OData response for {resource} is paginated")
    if len(records) != data["@odata.count"]:
        raise ValueError(
            f"OData response for {resource} is truncated: "
            f"{len(records)} of {data['@odata.count']}"
        )
    return records


def crawl_member(
    context: Context,
    positions: dict[str, tuple[Entity, PositionCategorisation]],
    record: dict[str, Any],
    terms: list[dict[str, Any]],
) -> None:
    phid = record.pop("PHID")
    person = context.make("Person")
    person.id = context.make_slug("person", phid)

    # pop without a default: a renamed/removed field crashes here rather than silently
    # dropping data, and audit_data surfaces any newly added field.
    family_name = record.pop("FamilyName")
    h.apply_name(
        person,
        first_name=record.pop("GivenName"),
        middle_name=record.pop("MiddleNames") or None,
        last_name=family_name,
    )
    preferred = record.pop("PreferredName")
    if preferred:
        # Always a parenthesised nickname, e.g. "(Tony)" on Anthony ABBOTT.
        if preferred.startswith("(") and preferred.endswith(")"):
            h.apply_name(
                person,
                first_name=preferred[1:-1],
                last_name=family_name,
                alias=True,
            )
        else:
            context.log.warning(
                "Unexpected PreferredName shape", phid=phid, value=preferred
            )

    h.apply_date(person, "birthDate", record.pop("DateOfBirth") or None)
    h.apply_date(person, "deathDate", record.pop("DateOfDeath") or None)
    birth_parts = [record.pop("PlaceOfBirth"), record.pop("StateOfBirth")]
    person.add("birthPlace", ", ".join(p for p in birth_parts if p) or None)
    person.add("birthCountry", record.pop("CountryOfBirth") or None)
    person.add("gender", record.pop("Gender") or None)
    person.add("political", record.pop("Party") or None)
    # Commonwealth Electoral Act 1918 s 163(1)(b) requires members of either federal
    # chamber to be Australian citizens; Constitution s 44(i) bars foreign citizens.
    # https://www.legislation.gov.au/C1918A00027/latest/text
    person.add("citizenship", "au")
    person.add("sourceUrl", f"https://handbook.aph.gov.au/Parliamentarian/{phid}")
    context.audit_data(record, ignore=INDIVIDUAL_IGNORE)

    has_occupancy = False
    for term in terms:
        chamber = term.pop("MpOrSenator")
        if chamber not in positions:
            raise ValueError(f"Unexpected MpOrSenator value: {chamber!r}")
        position, categorisation = positions[chamber]
        # The secondary date pair is unused by the source (always the sentinel); a real
        # value would be a second service period we'd otherwise drop.
        if term.pop("DateStart2") != ONGOING or term.pop("DateEnd2") != ONGOING:
            raise ValueError(f"Unexpected secondary service period for {phid}")
        end_date = term.pop("DateEnd1")
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            categorisation=categorisation,
            start_date=term.pop("DateStart1"),
            end_date=None if end_date == ONGOING else end_date,
        )
        state = term.pop("Value1")
        electorate = term.pop("Value2")
        if occupancy is not None:
            # House members sit for an electorate, senators for a state.
            occupancy.add("constituency", electorate or state)
            context.emit(occupancy)
            has_occupancy = True
        context.audit_data(term, ignore=SERVICE_IGNORE)

    if has_occupancy:
        context.emit(person)


def crawl(context: Context) -> None:
    house = h.make_position(
        context,
        name="Member of the Australian House of Representatives",
        country="au",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q18912794",
    )
    senate = h.make_position(
        context,
        name="Member of the Australian Senate",
        country="au",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q6814428",
    )
    positions: dict[str, tuple[Entity, PositionCategorisation]] = {}
    for chamber, position in (("Member", house), ("Senator", senate)):
        categorisation = categorise(context, position, default_is_pep=True)
        context.emit(position)
        positions[chamber] = (position, categorisation)

    # ROSTypeID 9 = "Parliamentary Service": one row per elected term, with chamber.
    service = fetch_collection(
        context, "recordsofservice", {"$filter": "ROSTypeID eq 9"}
    )
    terms_by_phid: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in service:
        terms_by_phid[row["PHID"]].append(row)

    for record in fetch_collection(context, "individuals", {}):
        terms = terms_by_phid.pop(record["PHID"], [])
        if not terms:
            context.log.info(
                "Individual without parliamentary service records", phid=record["PHID"]
            )
            continue
        crawl_member(context, positions, record, terms)

    if terms_by_phid:
        context.log.warning(
            "Parliamentary service records without a matching individual",
            phids=sorted(terms_by_phid.keys()),
        )
