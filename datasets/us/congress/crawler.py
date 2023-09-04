import os
from typing import Optional, List
from urllib.parse import urlencode, urljoin
from requests.exceptions import HTTPError

from zavod import helpers as h
from zavod import Context, Entity
from zavod import settings

API_KEY = os.environ.get("OPENSANCTIONS_US_CONGRESS_API_KEY")


def crawl_positions(context: Context, member, entity):
    terms: List[dict] = member.pop("terms")
    entities = []
    for term in terms:
        res = context.lookup("position", term["chamber"])
        position = h.make_position(context, res.name, country="us")
        occupancy = h.make_occupancy(
            context,
            entity,
            position,
            True,
            start_date=str(term.pop("startYear")),
            end_date=str(term.pop("endYear")) if "endYear" in term else None,
        )
        if occupancy:
            entities.append(position)
            entities.append(occupancy)
    return entities


def crawl_member(context: Context, bioguide_id: str):
    url = urljoin(context.data_url, bioguide_id)
    headers = {"x-api-key": API_KEY}
    member = context.fetch_json(url, headers=headers, cache_days=30)["member"]
    context.log.info("Crawling member: %s" % member.get("directOrderName"))

    person = context.make("Person")
    person.id = context.make_id(bioguide_id)
    person.add("birthDate", member.pop("birthYear"))
    person.add("name", member.pop("directOrderName"))
    person.add("firstName", member.pop("firstName"))
    person.add("lastName", member.pop("lastName"))
    person.add("middleName", member.pop("middleName", None))
    person.add("title", member.pop("honorificName", None))

    entities = crawl_positions(context, member, person)
    if entities:
        context.emit(person, target=True)
        for entity in entities:
            context.emit(entity)


def crawl(context: Context):
    if API_KEY is None:
        context.log.error("No API key set, skipping crawl.")
        return
    query = {"limit": 250}
    url = f"{context.data_url}?{urlencode(query)}"
    headers = {"x-api-key": API_KEY}
    while url:
        try:
            response = context.fetch_json(url, headers=headers, cache_days=1)
            url = response["pagination"].get("next", None)
            for member in response["members"]:
                if not member:  # There's one empty object in their results
                    continue
                crawl_member(context, member["bioguideId"])
        except HTTPError as err:
            if err.response.status_code == 429:
                context.log.info("Rate limit exceeded, stopping crawl.")
                break
            raise
