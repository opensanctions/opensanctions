from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise

# Page size accepted by the Members API. The API caps `take` at 20.
PAGE_SIZE = 20
TOPICS = ["gov.national", "gov.legislative"]


def crawl_member(
    context: Context,
    value: dict[str, Any],
) -> None:
    membership = value.pop("latestHouseMembership")
    start_date = membership["membershipStartDate"]
    end_date = membership["membershipEndDate"]

    if start_date and start_date < h.earliest_term_start(TOPICS):
        context.log.info(
            f"Skipping row with start date {start_date} outside coverage window"
        )
        return

    name = value.pop("nameDisplayAs")
    person = context.make("Person")
    person.id = context.make_id(name, value.pop("id"))

    person.add("name", name)
    person.add("name", value.pop("nameFullTitle"))
    person.add("name", value.pop("nameAddressAs"))
    person.add("name", value.pop("nameListAs"))
    person.add("gender", value.pop("gender"))
    person.add("political", value["latestParty"]["name"])
    # citizenship not required
    person.add("country", "gb")

    position = h.make_position(
        context,
        name="Member of the House of Commons",
        country="gb",
        topics=TOPICS,
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
        end_date=end_date,
        no_end_implies_current=True,
        categorisation=categorisation,
    )
    if occupancy is not None:
        occupancy.add("constituency", membership["membershipFrom"])
        context.emit(person)
        context.emit(position)
        context.emit(occupancy)

    # previous government posts
    bio_url = value.pop("thumbnailUrl").replace("Thumbnail", "biography")
    data_bio = context.fetch_json(bio_url)
    bio = data_bio["value"]
    gov_posts = bio["governmentPosts"]
    if gov_posts != [] and gov_posts is not None:
        for post in gov_posts:
            previous_position = h.make_position(
                context,
                name=post["name"],
                topics=["gov.national"],
                country="gb",
            )
            categorisation = categorise(context, previous_position)
            if not categorisation.is_pep:
                continue

            previous_occupancy = h.make_occupancy(
                context,
                person,
                previous_position,
                start_date=post["startDate"],
                end_date=post["endDate"],
                no_end_implies_current=True,
                categorisation=categorisation,
            )
            if previous_occupancy is not None:
                context.emit(person)
                context.emit(previous_position)
                context.emit(previous_occupancy)


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
