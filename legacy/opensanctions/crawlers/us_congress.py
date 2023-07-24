from functools import cache
import json
import os
from typing import Optional
from urllib.parse import urlencode, urljoin
from datetime import timedelta

from opensanctions import helpers as h
from opensanctions.core import Context
from zavod.entity import Entity
from zavod.parse.positions import make_position
from zavod import settings


API_KEY = os.environ.get("OPENSANCTIONS_US_CONGRESS_KEY")
LIMIT = 250
CACHE_DAYS = 5
AFTER_OFFICE = 5 * 365


# TODO: Copied from wd_pep, factor out
@cache
def to_date(days: int) -> str:
    dt = settings.RUN_TIME - timedelta(days=days)
    return dt.isoformat()[:10]


def make_occupancy(
    context: Context,
    person,
    position,
    start_date,
    end_date: Optional[str],
    no_end_implies_current: bool,
) -> Optional[Entity]:
    """Helper for making occupancies if they meet our criteria for PEP position occupancy"""
    # TODO: Change dates to be dates to generalise
    # TODO: The general version will need more of the logic from wd_peps than just this clause
    if end_date is None or end_date > to_date(AFTER_OFFICE):
        occupancy = context.make("Occupancy")
        parts = [person.id, position.id, start_date, end_date]
        occupancy.id = context.make_id(*parts)
        occupancy.add("holder", person)
        occupancy.add("post", position)
        occupancy.add("startDate", start_date)
        occupancy.add("endDate", end_date)
        if no_end_implies_current and not end_date:
            status = "Current"
        else:
            status = "Ended"
        occupancy.add("status", status)
        return occupancy


def crawl_positions(context, member, entity):
    terms: List[dict] = member.pop("terms")
    entities = []
    topics = set()
    for term in terms:
        res = context.lookup("position", term["chamber"])

        position = make_position(context, res.name, country="us")
        occupancy = make_occupancy(
            context,
            entity,
            position,
            str(term.pop("startYear")),
            str(term.pop("endYear", None)),
            True,
        )
        if occupancy:
            entities.append(position)
            entities.append(occupancy)
            topics.update(res.topics)
    return entities, topics


def crawl_member(context: Context, bioguide_id: str):
    url = urljoin(context.source.data.url, bioguide_id)
    headers = {"x-api-key": API_KEY}
    member = context.fetch_json(url, headers=headers, cache_days=CACHE_DAYS)["member"]

    person = context.make("Person")
    person.id = context.make_id(bioguide_id)
    person.add(
        "name", member.pop("directOrderName")
    )  # TODO: use name parts from full member endpoint
    person.add("country", "us")

    entities, topics = crawl_positions(context, member, person)
    if entities:
        person.add("topics", topics)
        context.emit(person, target=True)
        for entity in entities:
            context.emit(entity)


def fetch(context: Context, offset):
    query = {"limit": LIMIT, "offset": offset}
    url = f"{ context.source.data.url }?{ urlencode(query) }"
    headers = {"x-api-key": API_KEY}
    path = context.fetch_resource(f"members-{offset}.json", url, headers=headers)
    context.export_resource(path, title=context.SOURCE_TITLE + f"offset {offset}")
    with open(path, "r") as fh:
        return json.load(fh)["members"], offset + LIMIT


def crawl(context: Context):
    offset = 0
    while True:
        members, offset = fetch(context, offset)
        if not members:
            break

        for member in members:
            if member:  # There's one empty dict
                crawl_member(context, member["bioguideId"])


#

#
