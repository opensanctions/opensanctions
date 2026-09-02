from dataclasses import dataclass
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
PER_PAGE = 300
STATUS_SERVING = 0
STATUS_LEFT = 1


@dataclass(frozen=True)
class Term:
    period_id: int
    start_year: int
    end_year: int


def fetch_terms(context: Context) -> list[Term]:
    """Return every term the source lists, newest first.

    The period table is shared across chambers, so some of them serve no DPD members.
    """
    periods = zyte_api.fetch_json(context, PERIOD_URL, geolocation="id", cache_days=7)
    if not isinstance(periods, list):
        raise ValueError("Period endpoint did not return a list")
    terms = [Term(p["id"], p["startYear"], p["endYear"]) for p in periods]
    if len(terms) == 0:
        raise ValueError("Period endpoint returned no terms")
    terms.sort(key=lambda t: (t.end_year, t.start_year, t.period_id), reverse=True)
    return terms


def fetch_members(context: Context, term: Term) -> list[dict[str, Any]]:
    url = f"{context.data_url}?periodId={term.period_id}&page=1&perPage={PER_PAGE}"
    # The API host sits behind a WAF that rejects ordinary egress.
    members = zyte_api.fetch_json(context, url, geolocation="id", cache_days=7)
    if not isinstance(members, list):
        raise ValueError(f"Period {term.period_id} did not return a list of members")
    if len(members) == PER_PAGE:
        raise ValueError(f"Period {term.period_id} filled page 1; rows may be missing")
    return members


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    member: dict[str, Any],
    term: Term,
    is_current: bool,
) -> None:
    person = context.make("Person")
    person.id = context.make_slug(str(member.pop("id")))
    # Names carry honorifics and post-nominals; the affixes to strip are in the metadata.
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
    mandates = member.pop("memberPeriods")
    context.audit_data(
        member,
        [
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
        ],
    )

    occupancies: list[Entity] = []
    for mandate in mandates:
        if mandate["period"]["id"] != term.period_id:
            raise ValueError(
                f"Period {mandate['period']['id']} returned for {term.period_id}"
            )
        status = mandate["memberStatus"]
        if status not in (STATUS_SERVING, STATUS_LEFT):
            raise ValueError(f"Unknown memberStatus: {status!r}")
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            start_date=mandate["inaugurationDate"],
            period_start=str(term.start_year),
            period_end=str(term.end_year),
            status=(
                OccupancyStatus.ENDED if is_current and status == STATUS_LEFT else None
            ),
            categorisation=categorisation,
        )
        if occupancy is not None:
            occupancy.add("constituency", mandate["region"]["name"])
            occupancies.append(occupancy)
    # No occupancy means every mandate fell outside the PEP window.
    if len(occupancies) == 0:
        return
    for occupancy in occupancies:
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

    window_start_year = int(h.earliest_term_start(TOPICS)[:4])
    current_term: Term | None = None
    for term in fetch_terms(context):
        if term.end_year < window_start_year:
            continue
        members = fetch_members(context, term)
        if len(members) == 0:
            continue  # A term of one of the other chambers.
        if current_term is None:
            current_term = term
            if term.end_year < settings.RUN_TIME.year:
                raise ValueError(
                    f"Newest period {term.period_id} ended in {term.end_year}; "
                    "refusing to emit its members as current."
                )
        for member in members:
            crawl_member(
                context, position, categorisation, member, term, term is current_term
            )
        context.log.info(
            "Crawled term",
            id=term.period_id,
            years=f"{term.start_year}-{term.end_year}",
            members=len(members),
        )
    if current_term is None:
        raise ValueError("No term in the PEP window serves DPD members")
