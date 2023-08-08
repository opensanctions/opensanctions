import os
from typing import Optional, List
from urllib.parse import urlencode, urljoin

from zavod import helpers as h
from zavod import Context, Entity
from zavod import settings

API_KEY = os.environ.get("OPENSANCTIONS_US_CONGRESS_API_KEY")
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
    return None


def crawl_positions(context: Context, member, entity):
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
    person.add("birthDate", member.pop("birthYear"))
    person.add("name", member.pop("directOrderName"))
    person.add("firstName", member.pop("firstName"))
    person.add("lastName", member.pop("lastName"))
    person.add("middleName", member.pop("middleName", None))
    person.add("title", member.pop("honorificName", None))
    person.add("country", "us")

    entities, topics = crawl_positions(context, member, person)
    if entities:
        person.add("topics", topics)
        context.emit(person, target=True)
        for entity in entities:
            context.emit(entity)


def crawl(context: Context):
    if API_KEY is None:
        context.log.error("No API key set, skipping crawl.")
        return
    query = {"limit": LIMIT}
    url = f"{context.data_url}?{urlencode(query)}"
    headers = {"x-api-key": API_KEY}
    while url:
        response = context.fetch_json(url, headers=headers, cache_days=1)
        url = response["pagination"].get("next", None)
        for member in response["members"]:
            if not member:  # There's one empty object in their results
                continue
            crawl_member(context, member["bioguideId"])
