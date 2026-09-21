from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

from zavod.entity import Entity
from zavod.shed.trans import apply_translit_full_name
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import LangText

from zavod import Context
from zavod import helpers as h

INCEPTION_DATE = "1999-03-08"


@dataclass(frozen=True)
class Term:
    """A legislature of the Senate. `period_end` is None for the sitting legislature."""

    id: str
    period_start: str
    period_end: str | None


# The date each legislature first convened, keyed on its session id. The Senate's own
# history publishes these only in Khmer prose, so they are curated here:
# https://senate.gov.kh/about-senate/senate-history/
# When a new legislature appears in the search form, appending its convening date both
# opens its term and closes its predecessor's.
CONVENING_DATES = {
    "1": "1999-03-25",
    "2": "2006-03-20",
    "3": "2012-03-20",
    "4": "2018-04-23",
    "5": "2024-04-03",
}


def build_terms(convening_dates: dict[str, str]) -> dict[str, Term]:
    """Derive the term of each legislature: a legislature's mandate lapses when its
    successor convenes, and the latest legislature's term is still open."""
    sessions = sorted(convening_dates.items(), key=lambda item: item[1])
    ends = [start for _, start in sessions[1:]]
    return {
        session_id: Term(session_id, start, end)
        for (session_id, start), end in zip(sessions, ends + [None])
    }


TERMS = build_terms(CONVENING_DATES)


def discover_terms(context: Context) -> list[Term]:
    """Return the legislatures the search form offers."""
    doc = context.fetch_html(context.data_url, cache_days=1)
    options = h.xpath_elements(doc, '//select[@id="keysession"]/option')
    if len(options) == 0:
        raise ValueError("The senator search form lists no legislatures")
    terms: list[Term] = []
    for option in options:
        session_id = option.get("value")
        term = None if session_id is None else TERMS.get(session_id)
        if term is None:
            raise ValueError(
                f"No convening date known for legislature {session_id!r}. Add it to "
                "CONVENING_DATES from https://senate.gov.kh/about-senate/senate-history/"
            )
        terms.append(term)
    return terms


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
    if (result := context.lookup("type.date", value)) is not None:
        value = result.value
    if value is None:
        return None
    # The lower bound is the Senate's inception, not `term.period_start`: mandates
    # legitimately begin before a legislature first convenes (all of legislature 1
    # was appointed a week before convening, one senator of legislature 5 a month).
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
    assert name is not None
    dob = record.pop("dob")
    party = record.pop("party")

    person = context.make("Person")
    person.id = context.make_id(raw_name, dob, party)
    person.add(
        "name",
        name,
        lang="khm",
        original_value=raw_name if name != raw_name else None,
    )
    apply_translit_full_name(context, person, LangText(name, "khm"))
    person.add("gender", record.pop("gender"))
    h.apply_date(person, "birthDate", dob)
    person.add("political", party, lang="khm")
    biography = record.pop("biography")
    if biography.lower().endswith(".pdf"):
        person.add("sourceUrl", biography)
    # Senators must be Khmer citizens (Constitution of Cambodia, Article 34 (New)).
    # https://constitutionnet.org/sites/default/files/Cambodia%20Constitution.pdf
    person.add("citizenship", "kh")

    status = record.pop("status")
    assert status in ("0", "1")

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
        # The AJAX endpoint that populates the search page's senator directory.
        urljoin(context.data_url, "/wp-content/themes/senate/ajax/generalsearch.php"),
        method="POST",
        # The endpoint only answers to an in-page XHR: neither header has an `http`
        # metadata field, so both stay on the fetch call.
        headers={
            "X-Requested-With": "XMLHttpRequest",
            "Referer": context.data_url,
        },
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
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21295127",
        inception_date=[INCEPTION_DATE],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    for term in discover_terms(context):
        crawl_term(context, position, categorisation, term)
