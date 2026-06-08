from collections import defaultdict
from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise

# The data.govt.nz "Members of Parliament" dataset. Resource IDs are resolved by name at
# runtime rather than hardcoded: CKAN mints a new resource_id whenever a resource is
# re-uploaded, so a hardcoded ID would silently 404 after a routine refresh.
#
# Two resources are joined on the integer MemberID. The "Basic List of Current Members"
# resource is deliberately unused: it lacks MemberID and stores Māori macrons as "?", so
# it can be neither joined reliably nor trusted for names.
DATASET = "members-of-parliament"
ROSTER_NAME = (
    "Members of Parliament - full list"  # name, birth/death year, gender, ethnicity
)
TERMS_NAME = "Members of Parliament - Member Terms"  # one row per parliamentary term
API = "https://catalogue.data.govt.nz/api/3/action"
# Above the current row counts (1.5k roster, 5.3k terms); a truncated read fails loudly.
LIMIT = 20000


def fetch_action(context: Context, action: str, query: str) -> Any:
    url = f"{API}/{action}?{query}"
    data = zyte_api.fetch_json(context, url, geolocation="nz", cache_days=1)
    if not data.get("success"):
        raise ValueError(f"CKAN {action} request was not successful: {url}")
    return data["result"]


def resolve_resource_id(resources: list[dict[str, Any]], name: str) -> str:
    matches = [
        r for r in resources if r.get("name") == name and r.get("datastore_active")
    ]
    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one active datastore resource named {name!r}, "
            f"found {len(matches)}"
        )
    return str(matches[0]["id"])


def fetch_records(context: Context, resource_id: str) -> list[dict[str, Any]]:
    result = fetch_action(
        context, "datastore_search", f"resource_id={resource_id}&limit={LIMIT}"
    )
    records: list[dict[str, Any]] = result["records"]
    if len(records) != result["total"]:
        raise ValueError(
            f"Datastore read truncated for {resource_id}: "
            f"{len(records)} of {result['total']}"
        )
    return records


def iso_date(value: str | None) -> str | None:
    # Datastore dates look like "2023-10-14T00:00:00"; keep the calendar day.
    return value[:10] if value else None


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    member_id: int,
    record: dict[str, Any] | None,
    terms: list[dict[str, Any]],
) -> None:
    person = context.make("Person")
    person.id = context.make_slug("member", str(member_id))

    # The biographical roster occasionally lags the terms list for new members, so fall
    # back to the name carried on the term rows when a member is missing from it.
    name = record.pop("Name") if record is not None else terms[0]["Name"]
    last, _, first = name.partition(",")  # source format is "LASTNAME, First Middle"
    if first.strip():
        h.apply_name(person, first_name=first.strip(), last_name=last.strip())
    else:
        person.add("name", name.strip())

    # New Zealand citizenship is a legal requirement to be elected: Electoral Act 1993
    # s 47(3). https://www.legislation.govt.nz/act/public/1993/0087/latest/DLM308516.html
    person.add("citizenship", "nz")
    if record is not None:
        # pop without a default: a renamed/removed column crashes here rather than
        # silently dropping data, and audit_data surfaces any newly added column.
        person.add("gender", record.pop("Gender"))
        year_of_birth = record.pop("Year_of_Birth")
        h.apply_date(person, "birthDate", str(year_of_birth) if year_of_birth else None)
        year_of_death = record.pop("Year_of_Death")
        h.apply_date(person, "deathDate", str(year_of_death) if year_of_death else None)
        # Multiple ethnicities are comma-separated ("Māori, European"); a slash denotes a
        # single Stats NZ category ("Middle Eastern/Latin American/African"), so only
        # split on commas.
        person.add("ethnicity", h.multi_split(record.pop("Ethnicity"), [","]))
        context.audit_data(record, ignore=["_id", "MemberID"])

    has_occupancy = False
    for term in terms:
        person.add("political", term.pop("Party") or None)
        constituency = term.pop("Electorate_List") or None
        # make_occupancy gates each term on PEP duration, so old terms drop out and only
        # current or recently-ended ones survive — the historical multi-term pattern.
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            categorisation=categorisation,
            start_date=iso_date(term.pop("Date_Elected")),
            end_date=iso_date(term.pop("Date_Vacated")),
        )
        if occupancy is not None:
            occupancy.add("constituency", constituency)
            context.emit(occupancy)
            has_occupancy = True
        context.audit_data(
            term,
            ignore=[
                "_id",
                "MemberID",
                "Name",
                "Parliament",
                "Method_of_Election",
                "Reason_for_Leaving",
            ],
        )

    if has_occupancy:
        context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the New Zealand House of Representatives",
        country="nz",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q19899675",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    context.emit(position)

    resources = fetch_action(context, "package_show", f"id={DATASET}")["resources"]
    roster_id = resolve_resource_id(resources, ROSTER_NAME)
    terms_id = resolve_resource_id(resources, TERMS_NAME)

    terms_by_member: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for term in fetch_records(context, terms_id):
        terms_by_member[term["MemberID"]].append(term)

    sitting = {
        mid
        for mid, terms in terms_by_member.items()
        if any(not t["Date_Vacated"] for t in terms)
    }
    if not (115 <= len(sitting) <= 130):
        context.log.warning(
            "Sitting member count outside the expected ~120-123 range",
            count=len(sitting),
        )

    roster = {r["MemberID"]: r for r in fetch_records(context, roster_id)}
    # Iterate by member terms: a member with no term cannot hold a qualifying occupancy,
    # and a member present in terms but not yet in the roster must still be emitted.
    for member_id, terms in terms_by_member.items():
        crawl_member(
            context, position, categorisation, member_id, roster.get(member_id), terms
        )
