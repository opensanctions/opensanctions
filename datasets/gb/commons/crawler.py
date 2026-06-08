"""Crawler for current Members of the UK House of Commons.

Data is sourced from the official UK Parliament Members API. The Search
endpoint already returns all properties we need (name, gender, party,
constituency, membership start date), so we do not need to fetch the per-member
detail or biography endpoints.
"""

from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# Page size accepted by the Members API. The API caps `take` at 20.
PAGE_SIZE = 20

# Sanity band for the number of current MPs. The House of Commons currently has
# 650 seats; the count fluctuates slightly between general elections as seats
# fall vacant on death/resignation and are refilled at by-elections. If the
# source count drifts outside this band, the crawler should fail loudly so a
# maintainer can confirm the source is still healthy (e.g. a boundary change
# altering the number of seats).
MIN_EXPECTED = 600
MAX_EXPECTED = 660


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    value: dict[str, Any],
) -> None:
    """Emit a Person and Occupancy for a single MP record.

    Args:
        context: The crawl context.
        position: The shared Member of Parliament position entity.
        categorisation: The PEP categorisation for that position.
        value: The `value` object from a Search `items` element.
    """
    member_id = value.pop("id")
    if member_id is None:
        context.log.warning("Member without id", value=value)
        return

    person = context.make("Person")
    person.id = context.make_slug(member_id)

    # `nameDisplayAs` is the plain display name (e.g. "Ms Diane Abbott").
    # `nameFullTitle` carries honorifics and the "MP" suffix.
    person.add("name", value.pop("nameDisplayAs"))
    person.add("title", value.pop("nameFullTitle"))
    person.add("gender", value.pop("gender"))
    person.add("topics", "role.pep")

    source_url = f"https://members-api.parliament.uk/api/Members/{member_id}"
    person.add("sourceUrl", source_url)

    party = value.pop("latestParty")
    if party is not None:
        person.add("political", party.pop("name"))

    membership = value.pop("latestHouseMembership")
    start_date = None
    if membership is not None:
        start_date = membership.pop("membershipStartDate")

    # NOTE: We intentionally do NOT set `citizenship`. Membership of the UK
    # Parliament does not legally require British citizenship: citizens of the
    # Republic of Ireland and qualifying Commonwealth citizens resident in the
    # UK are also eligible to stand. See the Electoral Commission guidance and
    # the British Nationality Act 1981 as applied by the relevant electoral law.
    # UK Parliament is the documented citizenship exception in the crawler-pep
    # skill.

    # All person properties must be set before make_occupancy, which reads them
    # to determine PEP status.
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
    context.emit(occupancy)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the House of Commons",
        country="gb",
        # National parliament -> Wikidata QID for the UK MP position.
        wikidata_id="Q16707842",
    )
    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        return

    context.emit(position)

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
            # Freshness / sanity check: fail loudly if the number of current
            # MPs is outside the expected band (e.g. a boundary change altered
            # the number of seats, or the source is returning bad data).
            if not (MIN_EXPECTED <= total_results <= MAX_EXPECTED):
                raise ValueError(
                    f"Unexpected number of current MPs: {total_results} "
                    f"(expected between {MIN_EXPECTED} and {MAX_EXPECTED})"
                )

        items = data["items"]
        if len(items) == 0:
            break

        for item in items:
            crawl_member(context, position, categorisation, item["value"])
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
