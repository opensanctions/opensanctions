from functools import cache
import json
import os
from typing import Optional
from urllib.parse import urlencode, urljoin
from datetime import timedelta

from zavod import helpers as h
from zavod.context import Context
from zavod.entity import Entity
from zavod import settings

API_KEY = os.environ.get("OPENSANCTIONS_US_CONGRESS_KEY")
LIMIT = 250
CACHE_DAYS = 5
AFTER_OFFICE = 5 * 365


def make_occupancy(
    context: Context,
    person: Entity,
    position: Entity,
    start_date: str,
    end_date: Optional[str],
    no_end_implies_current: bool,
) -> Optional[Entity]:
    """Make occupancies if they meet our criteria for PEP position occupancy"""
    if end_date is None or end_date > h.backdate(settings.RUN_TIME, AFTER_OFFICE):
        occupancy = context.make("Occupancy")
        parts = [person.id, position.id, start_date, end_date]
        occupancy.id = context.make_id(*parts)
        occupancy.add("holder", person)
        occupancy.add("post", position)
        occupancy.add("startDate", start_date)
        if end_date:
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

        position = h.make_position(context, res.name, country="us")
        occupancy = make_occupancy(
            context,
            entity,
            position,
            str(term.pop("startYear")),
            str(term.pop("endYear")) if "endYear" in term else None,
            True,
        )
        if occupancy:
            entities.append(position)
            entities.append(occupancy)
            topics.update(res.topics)
    return entities, topics


def crawl_member(context: Context, bioguide_id: str):
    url = urljoin(context.data_url, bioguide_id)
    headers = {"x-api-key": API_KEY}
    member = context.fetch_json(url, headers=headers, cache_days=CACHE_DAYS)["member"]

    person = context.make("Person")
    person.id = context.make_id(bioguide_id)
    person.add("name", member.pop("directOrderName"))
    person.add("firstName", member.pop("firstName"))
    person.add("lastName", member.pop("lastName"))
    person.add("middleName", member.pop("middleName"))
    person.add("title", member.pop("honorificName"))
    person.add("country", "us")

    entities, topics = crawl_positions(context, member, person)
    if entities:
        person.add("topics", topics)
        context.emit(person, target=True)
        for entity in entities:
            context.emit(entity)


def fetch(context: Context, url):
    headers = {"x-api-key": API_KEY}
    r = context.http.get(url, headers=headers)
    return r.json()["members"], r.json()["pagination"].get("next", None)


def crawl(context: Context):
    query = {"limit": LIMIT}
    url = f"{ context.data_url }?{ urlencode(query) }"
    while url:
        members, url = fetch(context, url)

        for member in members:
            if member:  # There's one empty object in their results
                crawl_member(context, member["bioguideId"])
