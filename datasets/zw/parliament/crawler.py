from typing import Any

from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context, Entity
from zavod import helpers as h

TOPICS = ["gov.national", "gov.legislative"]


def crawl_member(
    context: Context,
    positions: dict[str, tuple[Entity, PositionCategorisation]],
    parliament: dict[str, Any],
    member: dict[str, Any],
) -> None:
    # Records of the 7th-9th Parliaments are shared: one record per person, with a
    # membership for each of those terms. Take only the membership of this parliament.
    memberships = [
        m
        for m in member.pop("parliamentMemberships")
        if m["parliamentId"] == parliament["id"]
    ]
    assert len(memberships) == 1, memberships
    # Memberships carry no personal dates: the start is the parliament's own, or blank.
    membership = memberships[0]
    assert membership["startDate"] in (None, parliament["startedOn"]), membership
    assert membership["endDate"] is None, membership

    person = context.make("Person")
    person.id = context.make_slug(str(member.pop("id")))
    raw_name = member.pop("fullName")
    name = h.strip_name_titles(context, raw_name)
    person.add("name", name, original_value=raw_name if name != raw_name else None)
    person.add("weakAlias", member.pop("nickName"))
    title = member.pop("title")
    person.add("title", context.lookup_value("title", title, title))
    person.add("gender", member.pop("gender"))
    h.apply_date(person, "birthDate", member.pop("dateOfBirth"))
    person.add("birthPlace", member.pop("placeOfBirth"))
    party = member.pop("party")
    if party is not None:
        res = context.lookup("party", party["name"], warn_unmatched=True)
        if res is not None:
            person.add("political", res.value)
    # Members of both houses must be registered voters (Constitution ss.121(1)-(2),
    # 125(1)), and only citizens may register (Fourth Schedule, para 1(1)).
    # https://www.constituteproject.org/constitution/Zimbabwe_2017?lang=en
    person.add("citizenship", "zw")

    # The record carries a single house, so for a member who changed chambers between
    # the 7th and 9th Parliaments, the older terms take the house of the latest one.
    position, categorisation = positions[member.pop("house")]
    if not categorisation.is_pep:
        return
    # The portal archives members who leave mid-term (recalls, deaths) without
    # recording when, so an archived member's open-ended membership is not current.
    archived = member.pop("archived")
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        period_start=parliament["startedOn"],
        period_end=parliament["endedOn"],
        no_end_implies_current=parliament["endedOn"] is None and not archived,
    )
    if occupancy is None:
        return
    constituency = member.pop("constituency")
    if constituency is not None:
        occupancy.add("constituency", constituency["name"])
    context.emit(occupancy)
    context.emit(position)
    context.emit(person)

    context.audit_data(
        member,
        ignore=[
            # The seat type (constituency, women's quota, chief, ...), which has no
            # FollowTheMoney property; both chambers' seats map to one position each.
            "entryType",
            # Free-text career notes: HTML biography, unstructured schooling.
            "biography",
            "education",
            "employment",
            "committeeMemberships",
            "maritalStatus",
            "religion",
            # Contact details and images are not extracted for PEPs.
            "email",
            "contactNumber",
            "image",
            "facebookUrl",
            "linkedinUrl",
            "twitterUrl",
            "instagramUrl",
            # Portal bookkeeping; the related objects are read above.
            "createdAt",
            "updatedAt",
            "constituencyId",
            "partyId",
        ],
    )


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
                "Parliament predates the PEP window", number=parliament["number"]
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
