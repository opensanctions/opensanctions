from typing import Dict, Any

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


IGNORE = [
    "image",
    "wikiTitle",
    "fullName",
    "pId",
    "showAs",
]


def crawl_member(context: Context, member: Dict[str, Any]) -> None:
    first_name = member.pop("firstName")
    last_name = member.pop("lastName")
    id = member.pop("memberCode")

    person = context.make("Person")
    person.id = context.make_id(first_name, last_name, id)
    h.apply_name(person, first_name=first_name, last_name=last_name)
    person.add("country", "ie")

    person.add("deathDate", member.pop("dateOfDeath", None))
    person.add("gender", member.pop("gender", None))
    person.add("sourceUrl", member.pop("uri", None))

    for membership in member.get("memberships", []):
        membership = membership["membership"]

        for party in membership["parties"]:
            person.add("political", party["party"]["showAs"])

        position = h.make_position(
            context,
            name="Member of the Irish Parliament",
            topics=["gov.national", "gov.legislative"],
            country=["ie"],
        )
        for represents in membership["represents"]:
            position.add("subnationalArea", represents["represent"]["showAs"])

        categorisation = categorise(context, position, is_pep=True)
        if not categorisation.is_pep:
            continue

        occupancy = h.make_occupancy(
            context,
            person,
            position,
            start_date=membership["dateRange"]["start"],
            end_date=membership["dateRange"]["end"],
            categorisation=categorisation,
        )
        if occupancy is not None:
            context.emit(person)
            context.emit(position)
            context.emit(occupancy)


def crawl(context: Context) -> None:
    all_members = []
    limit = 1000
    skip = 0

    # the API has a server-side cap of 1k results per request
    # we need a skip parameter to paginate records
    meta = context.fetch_json(context.data_url)
    total = meta["head"]["counts"]["memberCount"]

    while skip < total:
        batch = context.fetch_json(f"{context.data_url}?limit={limit}&skip={skip}")[
            "results"
        ]
        all_members.extend(batch)
        skip += limit

    for member in all_members:
        member = member["member"]
        crawl_member(context, member)
