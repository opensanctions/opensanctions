from typing import Any

from zavod import Context, settings
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import (
    OccupancyStatus,
    PositionCategorisation,
    categorise,
)

TOPICS = ["gov.national", "gov.legislative"]
PERIOD_URL = "https://service.dpd.go.id/anggota/api/v1/period"
PER_PAGE = 300
STATUS_SERVING = 0
STATUS_LEFT = 1
UNSET_DATE = "0001-01-01T00:00:00Z"
IGNORE = [
    "photoUrl",
    "photoThumbnailUrl",
    "religionName",
    "maritalStatus",
    "organStructures",
    "facebook",
    "instagram",
    "tiktok",
    "youtube",
    "twitter",
    "website",
]


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    member: dict[str, Any],
    period: dict[str, Any],
) -> None:
    person = context.make("Person")
    person.id = context.make_slug(str(member.pop("id")))
    raw_name = member.pop("fullName")
    clean_name = h.strip_name_titles(context, raw_name)
    person.add(
        "name",
        clean_name,
        lang="ind",
        original_value=raw_name if clean_name != raw_name else None,
    )
    person.add("gender", member.pop("gender"))
    person.add("birthPlace", member.pop("placeOfBirth"))
    h.apply_date(person, "birthDate", member.pop("dateOfBirth"))
    person.add("biography", member.pop("profile"))
    person.add("email", member.pop("email"))
    # DPD members must be Indonesian citizens (Law No. 7 of 2017, Article 182a).
    person.add("citizenship", "id")
    # memberPeriods spans a member's whole career, so pick out this term's mandate.
    mandates = [
        m for m in member.pop("memberPeriods") if m["period"]["id"] == period["id"]
    ]
    if len(mandates) > 1:
        # Many sole mandates are dateless too, so only drop a stub when one is dated.
        mandates = [m for m in mandates if m["inaugurationDate"] != UNSET_DATE]
    assert len(mandates) == 1, (person.id, len(mandates))
    mandate = mandates[0]
    status = mandate["memberStatus"]
    if status not in (STATUS_SERVING, STATUS_LEFT):
        context.log.warning(f"{person.id} has unknown memberStatus {status!r}")
    # In a running term, no end date implies still serving, so say when they left.
    left_early = status == STATUS_LEFT and period["endYear"] >= settings.RUN_TIME.year
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=mandate["inaugurationDate"],
        period_start=str(period["startYear"]),
        period_end=str(period["endYear"]),
        status=OccupancyStatus.ENDED if left_early else None,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", mandate["region"]["name"])

    context.emit(occupancy)
    context.emit(person)

    context.audit_data(member, IGNORE)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Regional Representative Council of Indonesia",
        country="id",
        topics=TOPICS,
        wikidata_id="Q21328635",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # The API host sits behind a WAF that rejects ordinary egress.
    periods = zyte_api.fetch_json(context, PERIOD_URL, geolocation="id", cache_days=7)
    earliest_year = int(h.earliest_term_start(TOPICS)[:4])
    for period in sorted(periods, key=lambda p: p["endYear"], reverse=True):
        if period["endYear"] < earliest_year:
            break
        url = f"{context.data_url}?periodId={period['id']}&perPage={PER_PAGE}"
        members = zyte_api.fetch_json(context, url, geolocation="id", cache_days=7)
        assert len(members) < PER_PAGE, (period["id"], len(members))
        term = f"{period['startYear']}-{period['endYear']}"
        context.log.info("Crawling term", term=term, members=len(members))
        for member in members:
            crawl_member(context, position, categorisation, member, period)
