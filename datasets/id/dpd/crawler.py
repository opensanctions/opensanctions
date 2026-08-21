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

PER_PAGE = 300

EMPTY_PERIODS_BEFORE_STOP = 3
MAX_PERIODS_PROBED = 40
STATUS_SERVING = 0
STATUS_LEFT = 1


@dataclass(frozen=True)
class Term:
    period_id: int
    start_year: int
    end_year: int
    members: list[dict[str, Any]]


def fetch_terms(context: Context) -> list[Term]:
    """Return every term the source serves DPD members for, newest first.

    The period table is shared across chambers, so the DPD's terms sit in a window of
    ids inside it, not starting at 1. Nothing here pins a term: the window and each
    term's years are read off the source, so an election needs no change to this code.
    """
    terms: list[Term] = []
    empty_run = 0
    for period_id in range(1, MAX_PERIODS_PROBED + 1):
        url = f"{context.data_url}?periodId={period_id}&page=1&perPage={PER_PAGE}"
        # The API host sits behind a WAF that rejects ordinary egress.
        members = zyte_api.fetch_json(context, url, geolocation="id", cache_days=7)
        # Also narrows `fetch_json`'s untyped return for the strict type check.
        if not isinstance(members, list):
            raise ValueError(f"Period {period_id} did not return a list of members")

        if len(members) == 0:
            if len(terms) == 0:
                continue  # Still below the window the DPD's terms occupy.
            empty_run += 1
            if empty_run >= EMPTY_PERIODS_BEFORE_STOP:
                break
            continue

        if len(members) == PER_PAGE:
            raise ValueError(f"Period {period_id} filled page 1; rows may be missing")
        empty_run = 0

        spans = {
            (mandate["period"]["startYear"], mandate["period"]["endYear"])
            for member in members
            for mandate in member["memberPeriods"]
            if mandate["period"]["id"] == period_id
        }
        if len(spans) != 1:
            raise ValueError(f"Period {period_id} has ambiguous years: {spans}")
        start_year, end_year = spans.pop()
        terms.append(Term(period_id, start_year, end_year, members))

    if len(terms) == 0:
        raise ValueError(f"No period up to {MAX_PERIODS_PROBED} serves DPD members")
    context.log.info(
        "Found terms",
        terms={t.period_id: f"{t.start_year}-{t.end_year}" for t in terms},
    )
    return sorted(terms, key=lambda t: t.period_id, reverse=True)


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    member: dict[str, Any],
    term: Term,
    sitting: bool,
) -> None:
    person = context.make("Person")
    person.id = context.make_slug(str(member.pop("id")))
    # Source names carry honorifics and academic post-nominals as part of the
    # name; strip the affixes declared in the dataset metadata.
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
    person.add("biography", member.pop("profile"))  # local lang
    person.add("email", member.pop("email"))
    # DPD candidates must be Indonesian citizens (Law No. 7 of 2017 on General
    # Elections, Article 182 letter a). https://peraturan.bpk.go.id/Details/37644
    person.add("citizenship", "id")

    mandates = member.pop("memberPeriods")

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
                OccupancyStatus.ENDED if sitting and status == STATUS_LEFT else None
            ),
            categorisation=categorisation,
        )
        if occupancy is not None:
            occupancy.add("constituency", mandate["region"]["name"])
            occupancies.append(occupancy)
    # A member whose only mandate falls outside the PEP window gets no occupancy.
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

    terms = fetch_terms(context)
    sitting = terms[0]
    if sitting.end_year < settings.RUN_TIME.year:
        raise ValueError(
            f"Newest period {sitting.period_id} ended in {sitting.end_year}: the "
            "source has no term in progress. Aborting as not to emit its members as "
            "current."
        )

    for term in terms:
        if term.end_year < int(h.earliest_term_start(TOPICS)[:4]):
            context.log.info(
                "Term ended before the PEP window; stopping",
                id=term.period_id,
                years=f"{term.start_year}-{term.end_year}",
            )
            break
        for member in term.members:
            crawl_member(
                context, position, categorisation, member, term, term is sitting
            )
        context.log.info(
            "Crawled term",
            id=term.period_id,
            years=f"{term.start_year}-{term.end_year}",
            members=len(term.members),
        )
