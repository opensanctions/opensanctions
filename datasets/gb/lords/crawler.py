from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise

# The UK Parliament Members API caps the `take` page size at 20 records.
PAGE_SIZE = 20
TOPICS = ["gov.national"]


def crawl_member(
    context: Context,
    item: dict[str, Any],
) -> None:
    member = item.pop("value")
    membership = member.pop("latestHouseMembership")
    start_date = membership.pop("membershipStartDate")
    end_date = membership.pop("membershipEndDate")
    if start_date and start_date < h.earliest_term_start(TOPICS):
        context.log.info(
            f"Skipping row with start date {start_date} outside coverage window"
        )
        return

    name = member.pop("nameDisplayAs")
    person = context.make("Person")
    person.id = context.make_id("person", name, member.pop("id"))

    person.add("name", name)
    person.add("name", member.pop("nameListAs", None))
    person.add("name", member.pop("nameFullTitle", None))
    person.add("name", member.pop("nameAddressAs", None))
    person.add("gender", member.pop("gender", None))
    party = member.pop("latestParty")
    if party is not None:
        person.add("political", party["name"])
    # citizenship not required
    person.add("country", "gb")

    position = h.make_position(
        context,
        name="Member of the House of Lords",
        country="gb",
        wikidata_id="Q18952564",
    )
    # hereditary, life peer, etc:
    position.add("notes", membership["membershipFrom"])
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        start_date=start_date,
        end_date=end_date,
    )
    if occupancy is not None:
        context.emit(occupancy)
        context.emit(position)
        context.emit(person)


def crawl(context: Context) -> None:
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
        items = data["items"]
        if len(items) == 0:
            break
        for item in items:
            crawl_member(context, item)
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
