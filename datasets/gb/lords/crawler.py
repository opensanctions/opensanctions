from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The UK Parliament Members API caps the `take` page size at 20 records.
PAGE_SIZE = 20
# The House of Lords currently has ~776 current members (including peers on
# leave of absence). This band is a sanity/freshness check: if the source count
# drifts far outside it (e.g. a large membership reform like the House of Lords
# (Hereditary Peers) Act 2025, or an API change), fail loudly so a human reviews
# the dataset rather than silently emitting a wrong-sized roster.
MIN_EXPECTED_MEMBERS = 600
MAX_EXPECTED_MEMBERS = 1200


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    item: dict[str, Any],
) -> None:
    # The Search envelope wraps each member in {"value": {...}, "links": [...]}.
    member = item.pop("value")
    item.pop("links", None)
    context.audit_data(item)

    member_id = member.pop("id")
    person = context.make("Person")
    person.id = context.make_slug("person", member_id)

    # Lords use a list-as name (e.g. "Adams of Craigielea, B.") and a display
    # name (e.g. "Baroness Adams of Craigielea"). The display name is the one
    # used publicly; the list-as form is kept as an alias.
    person.add("name", member.pop("nameDisplayAs"))
    person.add("alias", member.pop("nameListAs", None))
    person.add("alias", member.pop("nameFullTitle", None))
    person.add("alias", member.pop("nameAddressAs", None))
    person.add("gender", member.pop("gender", None))
    person.add(
        "sourceUrl",
        f"https://members.parliament.uk/member/{member_id}/career",
    )

    party = member.pop("latestParty", None)
    if party is not None:
        person.add("political", party.pop("name", None))

    membership = member.pop("latestHouseMembership")
    house = membership.pop("house")
    if house != 2:
        context.log.warning(
            "Member is not in the House of Lords (house != 2)",
            member_id=member_id,
            house=house,
        )
        return
    start_date = membership.pop("membershipStartDate", None)
    end_date = membership.pop("membershipEndDate", None)

    # NOTE: We deliberately do NOT set `citizenship`. Membership of the UK
    # Parliament does not legally require UK/Commonwealth/Irish citizenship in
    # the same definitive way most national legislatures do, so the UK is the
    # documented citizenship exception in the crawler-pep guidance. See
    # zavod/docs/peps.md and the crawler-pep skill.

    # IMPORTANT: set ALL person props BEFORE calling make_occupancy.
    # make_occupancy reads birthDate/deathDate from the entity to determine
    # PEP status.

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        start_date=start_date,
        end_date=end_date,
        propagate_country=True,
    )
    if occupancy is not None:
        context.emit(occupancy)
        # IMPORTANT: emit person AFTER make_occupancy — it adds role.pep to
        # person.topics.
        context.emit(person)

    # Fields not used for entity properties. `thumbnailUrl` is a photo link;
    # the membership sub-object carries audit metadata about the seat.
    context.audit_data(
        member,
        ignore=["thumbnailUrl"],
    )
    context.audit_data(
        membership,
        ignore=[
            "membershipFrom",
            "membershipFromId",
            "membershipEndReason",
            "membershipEndReasonNotes",
            "membershipEndReasonId",
            "membershipStatus",
        ],
    )


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the House of Lords",
        country="gb",
        wikidata_id="Q18952564",
    )
    categorisation = categorise(context, position, is_pep=True)
    context.emit(position)

    # TODO: Decide whether to filter out peers on leave of absence (who are not
    # currently sitting). The Search endpoint with IsCurrentMember=true includes
    # them. The list of peers on leave of absence is published separately at
    # https://www.parliament.uk/business/lords/whos-in-the-house-of-lords/leave-of-absence/
    # and would need to be cross-referenced if sitting-only membership is wanted.

    seen = 0
    total: int | None = None
    skip = 0
    while True:
        data = context.fetch_json(
            context.data_url,
            params={"skip": skip, "take": PAGE_SIZE},
            cache_days=1,
        )
        if total is None:
            total = data["totalResults"]
            # Freshness / sanity check: fail loudly on an unexpected roster size.
            if not (MIN_EXPECTED_MEMBERS <= total <= MAX_EXPECTED_MEMBERS):
                raise ValueError(
                    "Unexpected House of Lords member count "
                    f"({total}); expected between {MIN_EXPECTED_MEMBERS} and "
                    f"{MAX_EXPECTED_MEMBERS}. Review the source before trusting "
                    "this dataset."
                )

        items = data["items"]
        if len(items) == 0:
            break
        for item in items:
            crawl_member(context, position, categorisation, item)
            seen += 1

        skip += PAGE_SIZE
        if skip >= total:
            break

    if seen != total:
        context.log.warning(
            "Number of members processed does not match totalResults",
            seen=seen,
            total=total,
        )
