import json
import re
from dataclasses import dataclass
from typing import Any

from normality import squash_spaces
from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context, Entity
from zavod import helpers as h

CURRENT_MPS_URL = "https://www.parliament.gov.sg/mps/list-of-current-mps"
# "Halimah Yacob (Resigned on 7 August 2017, 13th Parliament)" - the only place the
# source records a member leaving before their Parliament was dissolved.
REGEX_RESIGNED = re.compile(r"\s*\(Resigned on (?P<date>[^,)]+), (?P<term>[^)]+)\)\s*$")
REGEX_TERM_RANGE = re.compile(r"^\((?P<start>[\d.]+)\s*-\s*(?P<end>[\d.]+)\)$")


@dataclass(frozen=True)
class Term:
    """One numbered Parliament, labelled the way the source labels it everywhere."""

    title: str
    start: str | None
    end: str | None


def fetch_page_data(context: Context, url: str) -> str:
    """Fetch the Next.js flight document a page renders itself from."""
    document = context.fetch_text(
        url,
        # Our HTTP cache keys on the URL alone, so this keeps the flight document and
        # the page's HTML apart.
        params={"_rsc": "1"},
        headers={"RSC": "1"},  # Serves the document itself, not the HTML embedding it.
        encoding="utf-8",  # `text/x-component` has no charset, so HTTP says Latin-1.
        cache_days=1,
    )
    assert document is not None, url
    return document


def decode_page_data(document: str, key: str) -> Any:
    """Decode the JSON value stored under `key`.

    The document is a stream of framed rows rather than one JSON value, so the value is
    located by its key and consumed from there.
    """
    marker = f'"{key}":'
    # Zero, or several with no way to tell which is the right copy.
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
    reported: int = roster["meta"]["filter_count"]
    if len(records) != reported:
        raise ValueError(f"Roster {key!r} holds {len(records)} of {reported} records")
    return records


def parse_terms(context: Context, options: list[dict[str, Any]]) -> dict[str, Term]:
    """Map each parliamentary session to the Parliament it was part of.

    The dropdown of Parliaments is the only place the source publishes term date ranges,
    and members reference their terms indirectly, by session.
    """
    sessions: dict[str, Term] = {}
    for index, option in enumerate(options):
        title = option.pop("title")
        content = option.pop("content")
        start, end = None, None
        if content is not None:
            match = REGEX_TERM_RANGE.match(squash_spaces(content))
            if match is None:
                raise ValueError(f"Unexpected {title} date range: {content!r}")
            start, end = match.group("start"), match.group("end")
        elif index != len(options) - 1:
            # Only the sitting Parliament, listed last, is published without a range.
            # Any other would make its members look like they never left.
            raise ValueError(f"Parliament without a date range: {title!r}")
        for session_id in option.pop("parliament_sessions"):
            sessions[session_id] = Term(title=title, start=start, end=end)
        context.audit_data(option, ignore=["id"])
    return sessions


def make_member(context: Context, published_name: str) -> Entity:
    """Create the person for a member, keyed on the name both rosters spell alike.

    Neither roster publishes an identifier the other shares, so the name is what makes a
    member listed in either of them one entity.
    """
    person = context.make("Person")
    person.id = context.make_id(published_name)
    h.apply_reviewed_name_string(
        context,
        person,
        string=h.strip_name_titles(context, published_name),
        llm_cleaning=True,
        lang="eng",
    )
    # Elected members and Nominated Members alike are Singapore citizens: Constitution
    # of the Republic of Singapore, Article 44(2)(a) and Fourth Schedule, paragraph 1.
    person.add("citizenship", "sg")
    return person


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    record: dict[str, Any],
    sessions: dict[str, Term],
) -> None:
    """Emit a member listed in the all-Parliaments roster, one occupancy per term."""
    published_name = record.pop("full_name")
    # A note on the published name is the only way the source records a member leaving
    # early; any other annotation is one this crawler cannot read.
    resigned = REGEX_RESIGNED.search(published_name)
    if resigned is not None:
        published_name = published_name[: resigned.start()]
    elif "(" in published_name or ")" in published_name:
        raise ValueError(f"Unhandled annotation in name: {published_name!r}")

    terms: set[Term] = set()
    for reference in record.pop("parliament_session"):
        session_id = reference["parliament_sessions_id"]
        if session_id not in sessions:
            raise ValueError(f"Unknown parliamentary session: {session_id!r}")
        # Members are listed once per session, so several references resolve to the same
        # Parliament.
        terms.add(sessions[session_id])
    # The label doubles as a seat type for Nominated and Non-Constituency Members, which
    # is published once per member and so belongs to no single term.
    party = context.lookup("party", record.pop("party_affliation"))
    # `legislative_assembly` flags service in the pre-1965 Legislative Assembly, outside
    # both this dataset and the PEP relevance window.

    person = make_member(context, published_name)
    if party is not None:
        person.add("political", party.values)

    occupancies: list[Entity] = []
    for term in terms:
        end_date = None
        if resigned is not None and term.title == resigned.group("term"):
            end_date = resigned.group("date")
        # The sitting Parliament is published without dates, so its members have none
        # and default to current. The roster is maintained: a member who resigned
        # dropped off the current one within weeks.
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            categorisation=categorisation,
            period_start=term.start,
            period_end=term.end,
            end_date=end_date,
        )
        if occupancy is not None:
            occupancies.append(occupancy)
    # Members whose every term is outside the PEP relevance window get no occupancy.
    if len(occupancies) == 0:
        return
    for occupancy in occupancies:
        context.emit(occupancy)
    context.emit(person)

    context.audit_data(record, ignore=["photo", "status", "legislative_assembly"])


def crawl_sitting_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    record: dict[str, Any],
) -> None:
    """Emit what only the current roster knows about a sitting member.

    The all-Parliaments roster covers the sitting Parliament as well, and both rosters
    produce the same person and the same undated occupancy for it, so this adds to those
    rather than duplicating them.

    The record carries 61 fields, nearly all of them content-management bookkeeping or
    facts outside this dataset's scope: contact details, committee seats, and the dated
    constituency and office-holding histories, which deserve entities of their own.
    """
    person = make_member(context, record["full_name"])
    person.add("sourceUrl", f"{CURRENT_MPS_URL}/mp/details/{record['url']}")
    # Only the current roster publishes a year of birth, and not for every member.
    h.apply_date(person, "birthDate", record["year_of_birth"])

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    # Where the source records the seat type of Nominated and Non-Constituency Members,
    # in place of a constituency.
    occupancy.add("constituency", record["constituency"]["constituency"])
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

    document = fetch_page_data(context, context.data_url)
    sessions = parse_terms(context, decode_page_data(document, "options"))
    for record in decode_roster(document, "mps"):
        crawl_member(context, position, categorisation, record, sessions)

    # The sitting members, who the all-Parliaments roster covers too, but without their
    # constituency, profile or year of birth.
    document = fetch_page_data(context, CURRENT_MPS_URL)
    for record in decode_roster(document, "initialMPs"):
        crawl_sitting_member(context, position, categorisation, record)
