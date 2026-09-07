import json
import re
from typing import Any, NamedTuple

from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context, Entity
from zavod import helpers as h

CURRENT_MPS_URL = "https://www.parliament.gov.sg/mps/list-of-current-mps"
# e.g. "Halimah Yacob (Resigned on 7 August 2017, 13th Parliament)".
REGEX_RESIGNED = re.compile(r"\s*\(Resigned on (?P<date>[^,)]+), (?P<term>[^)]+)\)\s*$")
REGEX_TERM_RANGE = re.compile(r"^\((?P<start>[\d.]+)\s*-\s*(?P<end>[\d.]+)\)$")


class Term(NamedTuple):
    """One numbered Parliament, labelled the way the source labels it everywhere."""

    title: str
    start: str | None
    end: str | None


def fetch_page_data(context: Context, url: str) -> str:
    """Fetch the Next.js flight document a page renders itself from."""
    document = context.fetch_text(
        url,
        # Our cache keys on the URL alone; this parts the document from the HTML.
        params={"_rsc": "1"},
        headers={"RSC": "1"},  # Serves the document itself, not the HTML embedding it.
        encoding="utf-8",  # `text/x-component` has no charset, so HTTP says Latin-1.
        cache_days=1,
    )
    assert document is not None, url
    return document


def decode_page_data(document: str, key: str) -> Any:
    """Decode the JSON value stored under `key`.

    The document is framed rows, not one JSON value, so the value is read from its key.
    """
    marker = f'"{key}":'
    if document.count(marker) != 1:
        raise ValueError(f"Page data has {document.count(marker)} {key!r} values")
    value, _ = json.JSONDecoder().raw_decode(
        document, document.find(marker) + len(marker)
    )
    return value


def decode_roster(document: str, key: str) -> list[dict[str, Any]]:
    """Decode a roster, checking it against the record count the page reports."""
    roster = decode_page_data(document, key)
    records: list[dict[str, Any]] = roster["data"]
    assert len(records) == roster["meta"]["filter_count"], key
    return records


def parse_terms(context: Context, options: list[dict[str, Any]]) -> dict[str, Term]:
    """Map each parliamentary session to the Parliament it was part of.

    The dropdown is the only place the source publishes term date ranges, and members
    reference their terms by session. Only the current Parliament, listed last, has no
    range; any other without one would show its members as never having left.
    """
    sessions: dict[str, Term] = {}
    for option in options:
        title = option.pop("title")
        content = option.pop("content")
        if content is None:
            if option is not options[-1]:
                raise ValueError(f"Parliament without a date range: {title!r}")
            term = Term(title, None, None)
        else:
            match = REGEX_TERM_RANGE.match(content)
            if match is None:
                raise ValueError(f"Unexpected {title} date range: {content!r}")
            term = Term(title, match.group("start"), match.group("end"))
        for session_id in option.pop("parliament_sessions"):
            sessions[session_id] = term
        context.audit_data(option, ignore=["id"])
    return sessions


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    record: dict[str, Any],
    sessions: dict[str, Term],
    current_mps: dict[str, dict[str, Any]],
) -> None:
    """Emit one member, with an occupancy per Parliament they sat in.

    Members are keyed on their published name: neither roster publishes an identifier
    the other shares, so the name both merges the two listings and joins them here.
    """
    published_name = record.pop("full_name")
    # A note on the name is the only record of a member leaving early. Any other
    # annotation is unreadable here, but the member is still worth emitting as given.
    resigned = REGEX_RESIGNED.search(published_name)
    resigned_term, resigned_date = None, None
    if resigned is not None:
        published_name = published_name[: resigned.start()]
        resigned_term = resigned.group("term")
        resigned_date = resigned.group("date")
    elif "(" in published_name or ")" in published_name:
        context.log.warning("Unhandled annotation in name", name=published_name)

    # Members are listed once per session, so several resolve to the same Parliament.
    terms: set[Term] = set()
    for reference in record.pop("parliament_session"):
        terms.add(sessions[reference["parliament_sessions_id"]])
    # Doubles as a seat type for Nominated and Non-Constituency Members. Published once
    # per member, so it belongs to the person rather than to any one term.
    party = context.lookup("party", record.pop("party_affliation"))
    # `legislative_assembly` flags pre-1965 service, outside this dataset either way.
    context.audit_data(record, ignore=["photo", "status", "legislative_assembly"])

    person = context.make("Person")
    person.id = context.make_id(published_name)
    person.add("name", h.strip_name_titles(context, published_name))
    # Elected or nominated, a member is a Singapore citizen: Constitution of the
    # Republic of Singapore, Article 44(2)(a) and Fourth Schedule, paragraph 1.
    person.add("citizenship", "sg")
    if party is not None:
        person.add("political", party.values)

    # The current roster's other 53 fields are CMS bookkeeping or out of scope: contact
    # details, committee seats, dated constituency and office-holding histories.
    # A member who returned after resigning has a record per Parliament, so the current
    # record goes to whichever holds the undated term, not to the first one seen.
    current = None
    if any(term.start is None and term.end is None for term in terms):
        current = current_mps.pop(published_name, None)
    if current is not None:
        person.add("sourceUrl", f"{CURRENT_MPS_URL}/mp/details/{current['url']}")
        person.add("sourceUrl", current["government_directory_link"])
        # Only that roster publishes a year of birth, and not for every member.
        h.apply_date(person, "birthDate", current["year_of_birth"])
        person.add("name", current["name_in_chinese"], lang="zho")
        person.add("name", current["name_in_tamil"], lang="tam")

    occupancies: list[Entity] = []
    for term in terms:
        # The current Parliament has no dates, so its members stay open-ended. The
        # roster is kept up: a member who resigned dropped off it within weeks.
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            categorisation=categorisation,
            period_start=term.start,
            period_end=term.end,
            end_date=resigned_date if term.title == resigned_term else None,
        )
        if occupancy is None:
            continue
        # Holds the seat type for Nominated and Non-Constituency Members instead of a
        # constituency, and is published for the current term only.
        if current is not None and term.start is None and term.end is None:
            occupancy.add("constituency", current["constituency"]["constituency"])
        occupancies.append(occupancy)
    # Members whose every term is outside the PEP relevance window get no occupancy.
    if len(occupancies) == 0:
        return
    for occupancy in occupancies:
        context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Parliament of Singapore",
        country="sg",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21294917",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # Covered by the all-Parliaments roster too, but without constituency or birth year.
    document = fetch_page_data(context, CURRENT_MPS_URL)
    current_mps = {r["full_name"]: r for r in decode_roster(document, "initialMPs")}

    document = fetch_page_data(context, context.data_url)
    sessions = parse_terms(context, decode_page_data(document, "options"))
    for record in decode_roster(document, "mps"):
        crawl_member(context, position, categorisation, record, sessions, current_mps)
    # Every current member is listed in the all-Parliaments roster under the same name.
    assert len(current_mps) == 0, sorted(current_mps)
