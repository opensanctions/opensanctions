from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise

# Page size accepted by the Members API. The API caps `take` at 20.
PAGE_SIZE = 20


def crawl_member(
    context: Context,
    value: dict[str, Any],
) -> None:
    name = value.pop("nameDisplayAs")
    person = context.make("Person")
    person.id = context.make_id(name, value.pop("id"))

    person.add("name", name)
    person.add("title", value.pop("nameFullTitle"))
    person.add("gender", value.pop("gender"))
    person.add("topics", "role.pep")
    # British citizenship isn't required: citizens of the
    # Republic of Ireland and qualifying Commonwealth citizens resident in the
    # UK are also eligible to stand. See the Electoral Commission guidance and
    # the British Nationality Act 1981
    person.add("country", "gb")

    party = value.pop("latestParty")
    if party is not None:
        person.add("political", party.pop("name"))

    membership = value.pop("latestHouseMembership")
    start_date = None
    if membership is not None:
        start_date = membership.pop("membershipStartDate")

    position = h.make_position(
        context,
        name="Member of the House of Commons",
        country="gb",
        wikidata_id="Q16707842",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=start_date,
        categorisation=categorisation,
    )
    if occupancy is None:
        return

    context.emit(person)
    context.emit(position)
    context.emit(occupancy)


def crawl(context: Context) -> None:
    skip = 0
    total_results = None
    seen = 0
    while True:
        data = context.fetch_json(
            context.data_url,
            params={"skip": skip, "take": PAGE_SIZE},
            cache_days=1,
        )
        if total_results is None:
            total_results = data["totalResults"]

        items = data["items"]
        if len(items) == 0:
            break

        for item in items:
            crawl_member(context, item["value"])
            seen += 1

        skip += PAGE_SIZE
        if skip >= total_results:
            break

    if seen != total_results:
        context.log.warning(
            "Consumed member count does not match totalResults",
            seen=seen,
            total_results=total_results,
        )
