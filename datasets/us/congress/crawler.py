import os
from typing import List
from urllib.parse import urlencode, urljoin
from requests.exceptions import HTTPError

from zavod import helpers as h
from zavod import Context
from zavod.logic.pep import categorise

API_KEY = os.environ.get("OPENSANCTIONS_US_CONGRESS_API_KEY")
IGNORE = [
    "addressInformation",
    "cosponsoredLegislation",
    "depiction",
    "district",
    "invertedOrderName",
    "officialWebsiteUrl",
    "partyHistory",
    "sponsoredLegislation",
    "state",
    "updateDate",
    "bioguideId",
    "currentMember",
    "suffixName",
    "leadership",
    "nickName",
]


def crawl_positions(context: Context, member, entity):
    terms: List[dict] = member.pop("terms")
    entities = []
    for term in terms:
        res = context.lookup("position", term["chamber"])
        if res is None:
            context.log.warn("Unknown chamber", chamber=term["chamber"])
            continue
        position = h.make_position(context, res.name, country="us")
        categorisation = categorise(context, position)
        if categorisation.is_pep:
            occupancy = h.make_occupancy(
                context,
                entity,
                position,
                True,
                start_date=str(term.pop("startYear")),
                end_date=str(term.pop("endYear")) if "endYear" in term else None,
                categorisation=categorisation,
            )
            if occupancy:
                entities.append(position)
                entities.append(occupancy)
    return entities


def crawl_member(context: Context, bioguide_id: str):
    url = urljoin(context.data_url, bioguide_id)
    assert API_KEY is not None, "No $OPENSANCTIONS_US_CONGRESS_API_KEY key set."
    headers = {"x-api-key": API_KEY}
    member = context.fetch_json(url, headers=headers, cache_days=30)["member"]
    context.log.info("Crawling member: %s" % member.get("directOrderName"))

    person = context.make("Person")
    person.id = context.make_id(bioguide_id)
    h.apply_date(person, "birthDate", member.pop("birthYear", None))
    h.apply_date(person, "deathDate", member.pop("deathYear", None))
    person.add("name", member.pop("directOrderName"))
    person.add("firstName", member.pop("firstName"))
    person.add("lastName", member.pop("lastName"))
    person.add("middleName", member.pop("middleName", None))
    person.add("title", member.pop("honorificName", None))

    entities = crawl_positions(context, member, person)

    context.audit_data(member, ignore=IGNORE)
    if entities:
        context.emit(person)
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
