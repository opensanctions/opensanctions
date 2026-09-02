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
# Sibling of the member endpoint in `data.url`, listing every term the API knows.
PERIOD_URL = "https://service.dpd.go.id/anggota/api/v1/period"
# The chamber seats 152, so one page takes a whole term.
PER_PAGE = 300
STATUS_SERVING = 0
STATUS_LEFT = 1
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
    # Names carry honorifics and post-nominals; the affixes are in the metadata.
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
    context.audit_data(member, IGNORE)

    if len(mandates) != 1:
        context.log.warning(
            f"{person.id} has {len(mandates)} mandates in {period['id']}"
        )
    mandate = mandates[0]
    if mandate["memberStatus"] not in (STATUS_SERVING, STATUS_LEFT):
        raise ValueError(f"Unknown memberStatus: {mandate['memberStatus']!r}")
    # A term that is still running implies everyone in it is still serving, so
    # someone who left it early needs saying so explicitly.
    left_early = (
        mandate["memberStatus"] == STATUS_LEFT
        and period["endYear"] >= settings.RUN_TIME.year
    )
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
    # No occupancy means the term has aged out of the PEP window.
    if occupancy is None:
        return
    occupancy.add("constituency", mandate["region"]["name"])
    context.emit(occupancy)
    context.emit(person)


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
    # The period table is shared across chambers, so those of the others serve
    # no members here.
    for period in sorted(periods, key=lambda p: p["endYear"], reverse=True):
        if period["endYear"] < earliest_year:
            break
        url = f"{context.data_url}?periodId={period['id']}&perPage={PER_PAGE}"
        members = zyte_api.fetch_json(context, url, geolocation="id", cache_days=7)
        assert len(members) < PER_PAGE, (period["id"], len(members))
        context.log.info(
            "Crawling term",
            id=period["id"],
            years=f"{period['startYear']}-{period['endYear']}",
            members=len(members),
        )
        for member in members:
            crawl_member(context, position, categorisation, member, period)
