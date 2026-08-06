from dataclasses import dataclass
from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

TOPICS = ["gov.national", "gov.legislative"]

# The senator directory is populated by an AJAX endpoint keyed on the legislature
# ("session") number. The search form on this page lists the legislatures on offer.
SEARCH_URL = "https://senate.gov.kh/search-senator-all/"

# The endpoint only answers to an in-page XHR: neither header has an `http` metadata
# field, so both stay on the fetch call.
HEADERS = {
    "X-Requested-With": "XMLHttpRequest",
    "Referer": SEARCH_URL,
}

# The source uses an all-zero date as its placeholder for an unknown date of birth,
# mandate start or mandate end.
DATE_PLACEHOLDER = "0000-00-00"

# The Senate was established by the constitutional amendment that entered into force on
# this date, so no mandate can have started earlier.
INCEPTION_DATE = "1999-03-08"


@dataclass(frozen=True)
class Term:
    """A legislature of the Senate. `period_end` is None for the sitting legislature."""

    id: str
    period_start: str
    period_end: str | None


# The date each legislature first convened — and with it the date on which the preceding
# legislature's mandate lapsed — as published in the Senate's own history:
# https://senate.gov.kh/about-senate/senate-history/
TERMS = {
    term.id: term
    for term in [
        Term("1", "1999-03-25", "2006-03-20"),
        Term("2", "2006-03-20", "2012-03-20"),
        Term("3", "2012-03-20", "2018-04-23"),
        Term("4", "2018-04-23", "2024-04-03"),
        Term("5", "2024-04-03", None),
    ]
}


def discover_terms(context: Context) -> list[Term]:
    """Return the legislatures the search form offers, newest first."""
    doc = context.fetch_html(SEARCH_URL, cache_days=1)
    options = h.xpath_elements(doc, '//select[@id="keysession"]/option')
    if len(options) == 0:
        raise ValueError("The senator search form lists no legislatures")
    terms: list[Term] = []
    for option in options:
        session_id = option.get("value")
        term = None if session_id is None else TERMS.get(session_id)
        if term is None:
            raise ValueError(
                f"No dates known for legislature {session_id!r}. Add them from "
                "https://senate.gov.kh/about-senate/senate-history/"
            )
        terms.append(term)
    return sorted(terms, key=lambda t: t.period_start, reverse=True)


def mandate_date(
    context: Context, value: str | None, term: Term, senator: str
) -> str | None:
    """Return a senator's own mandate date, or None where the source's value is
    impossible for the legislature in question.

    Some records hold the senator's date of birth in the mandate `start` field, and one
    holds the date the *following* legislature convened, so a date that falls outside
    the window in which a mandate of this legislature could run is dropped and the term
    bounds are left to date the occupancy on their own.
    """
    if value is None or value == DATE_PLACEHOLDER:
        return None
    if value < INCEPTION_DATE or (
        term.period_end is not None and value >= term.period_end
    ):
        context.log.info(
            "Ignoring mandate date from outside the legislature",
            session=term.id,
            senator=senator,
            date=value,
        )
        return None
    return value


def crawl_senator(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    term: Term,
    record: dict[str, Any],
) -> None:
    raw_name = record.pop("name")
    name = h.strip_name_titles(context, raw_name)
    if name is None:
        return
    dob = record.pop("dob")
    if dob == DATE_PLACEHOLDER:
        dob = None

    person = context.make("Person")
    # The source's row ids are per-legislature: a senator who served several
    # legislatures gets a fresh `str_id` in each, so keying the person on it would
    # split them into one entity per term. Their name and date of birth identify them
    # across legislatures instead.
    person.id = context.make_id(name, dob)
    person.add(
        "name",
        name,
        lang="khm",
        original_value=raw_name if name != raw_name else None,
    )
    person.add("gender", record.pop("gender"))
    h.apply_date(person, "birthDate", dob)
    person.add("political", record.pop("party"), lang="khm")
    # A senator's profile document, where they have one. Records without one point at
    # the bare directory that holds the documents, which is not about the senator.
    biography = record.pop("biography")
    if biography.lower().endswith(".pdf"):
        person.add("sourceUrl", biography)
    # Senators must be Khmer citizens (Constitution of Cambodia, Article 34 (New)).
    # https://constitutionnet.org/sites/default/files/Cambodia%20Constitution.pdf
    person.add("citizenship", "kh")

    status = record.pop("status")
    if status not in ("0", "1"):
        raise ValueError(f"Unexpected senator status {status!r} for {name}")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        period_start=term.period_start,
        period_end=term.period_end,
        start_date=mandate_date(context, record.pop("start"), term, name),
        end_date=mandate_date(context, record.pop("end"), term, name),
        # Per occupancy, not per dataset: only the sitting legislature is still open,
        # and a senator it marks as no longer serving isn't current either.
        no_end_implies_current=term.period_end is None and status == "1",
    )
    context.audit_data(
        record,
        ignore=[
            # A per-legislature row id, not a stable identifier for the senator.
            "str_id",
            "photo",
            "phone",
            # The row id of the senator who took over the seat, or vice versa.
            "replaceby",
            "session",
            "session_id",
        ],
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)


def crawl_term(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    term: Term,
) -> None:
    records: list[dict[str, Any]] = context.fetch_json(
        context.data_url,
        method="POST",
        headers=HEADERS,
        # The form's two status checkboxes combine into `chck_status`: 1 returns the
        # senators still serving in the legislature, 2 those who left it early, 3 both.
        data={"session": term.id, "keyword": "", "chck_status": "3"},
        cache_days=1,
    )
    if len(records) == 0:
        raise ValueError(f"No senators returned for legislature {term.id}")
    for record in records:
        crawl_senator(context, position, categorisation, term, record)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Senate of Cambodia",
        country="kh",
        topics=TOPICS,
        wikidata_id="Q21295127",
        inception_date=[INCEPTION_DATE],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    cutoff = h.earliest_term_start(TOPICS)
    for term in discover_terms(context):
        # ISO date strings compare correctly here.
        if term.period_end is not None and term.period_end < cutoff:
            context.log.info(
                "Legislature predates the PEP relevance window; skipping",
                session=term.id,
                period_end=term.period_end,
            )
            break
        crawl_term(context, position, categorisation, term)
