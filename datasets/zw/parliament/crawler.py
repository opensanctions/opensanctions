from typing import Any

from zavod.extract import zyte_api
from zavod.stateful.positions import (
    OccupancyStatus,
    PositionCategorisation,
    categorise,
)

from zavod import Context, Entity
from zavod import helpers as h

TOPICS = ["gov.national", "gov.legislative"]
IGNORE = [
    "parliamentMemberships",
    "biography",
    "entryType",
    "education",
    "employment",
    "committeeMemberships",
    "maritalStatus",
    "religion",
    "email",
    "contactNumber",
    "image",
    "facebookUrl",
    "linkedinUrl",
    "twitterUrl",
    "instagramUrl",
    "createdAt",
    "updatedAt",
    "constituencyId",
    "partyId",
]


def crawl_member(
    context: Context,
    positions: dict[str, tuple[Entity, PositionCategorisation]],
    parliament: dict[str, Any],
    member: dict[str, Any],
) -> None:
    person = context.make("Person")
    person.id = context.make_slug(member.pop("id"))
    person.add("name", member.pop("fullName"))
    h.apply_date(person, "birthDate", member.pop("dateOfBirth"))
    person.add("weakAlias", member.pop("nickName"))
    title = member.pop("title")
    title_lookup = context.lookup("title", title)
    person.add("title", title if title_lookup is None else title_lookup.value)
    person.add("gender", member.pop("gender"))
    person.add("birthPlace", member.pop("placeOfBirth"))
    person.add("citizenship", "zw")
    party = member.pop("party")
    if party is not None:
        person.add(
            "political",
            context.lookup_value("party", party["name"], warn_unmatched=True),
        )

    # The record carries a single house, so for a member who changed chambers between
    # the 7th and 9th Parliaments, the older terms take the house of the latest one.
    position, categorisation = positions[member.pop("house")]
    if not categorisation.is_pep:
        return
    # The portal archives members who leave the sitting parliament (recalls, deaths)
    # without recording when: the source says they left, so mark them ended.
    left_early = member.pop("archived") and parliament["endedOn"] is None
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        period_start=parliament["startedOn"],
        period_end=parliament["endedOn"],
        status=OccupancyStatus.ENDED if left_early else None,
    )
    if occupancy is None:
        return

    constituency = member.pop("constituency")
    if constituency is not None:
        occupancy.add("constituency", constituency["name"])

    context.emit(occupancy)
    context.emit(position)
    context.emit(person)

    context.audit_data(member, IGNORE)


def crawl(context: Context) -> None:
    positions: dict[str, tuple[Entity, PositionCategorisation]] = {}
    for house, name, qid in [
        (
            "NATIONAL_ASSEMBLY",
            "Member of the National Assembly of Zimbabwe",
            "Q21296472",
        ),
        ("SENATE", "Member of the Senate of Zimbabwe", "Q21295155"),
    ]:
        position = h.make_position(
            context, name, wikidata_id=qid, country="zw", topics=TOPICS, lang="eng"
        )
        positions[house] = (position, categorise(context, position))

    # The portal sits behind a Cloudflare JS challenge that blocks plain HTTP clients.
    parliaments = zyte_api.fetch_json(
        context, f"{context.data_url}/parliament", cache_days=1
    )["parliaments"]
    sitting = [p for p in parliaments if p["endedOn"] is None]
    assert len(sitting) == 1, sitting

    cutoff = h.earliest_term_start(TOPICS)
    # Newest first, so the first term out of the PEP window ends the loop.
    for parliament in sorted(parliaments, key=lambda p: p["startedOn"], reverse=True):
        if parliament["endedOn"] is not None and parliament["endedOn"] < cutoff:
            context.log.info(
                "Parliament predates the PEP window", parliament_no=parliament["number"]
            )
            break
        # `parliaments` filters by the internal id, not the parliament's number.
        # Without an `archived` filter, the response includes archived members.
        url = f"{context.data_url}/member?page=1&limit=1000&parliaments={parliament['id']}"
        data = zyte_api.fetch_json(context, url, cache_days=1)
        pagination = data["pagination"]
        assert pagination["pages"] == 1, pagination
        for member in data["members"]:
            crawl_member(context, positions, parliament, member)
