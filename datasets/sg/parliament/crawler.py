import json
import re
from dataclasses import dataclass
from typing import Any

from normality import squash_spaces
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context
from zavod import helpers as h

# The sitting members, who the all-Parliaments roster covers too, but without their
# constituency, profile or year of birth.
CURRENT_MPS_URL = "https://www.parliament.gov.sg/mps/list-of-current-mps"
MP_DETAIL_URL = "https://www.parliament.gov.sg/mps/list-of-current-mps/mp/details/%s"
FLIGHT_PUSH = "self.__next_f.push("
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


def fetch_flight_document(context: Context, url: str) -> str:
    """Fetch a page and reassemble the Next.js flight document it embeds.

    Both rosters render client-side: the server ships their data as a sequence of
    `self.__next_f.push([1, "<chunk>"])` calls whose string chunks concatenate, in
    document order, into one flight document. Chunk boundaries fall at arbitrary
    offsets, so nothing can be decoded before they are joined.
    """
    text = context.fetch_text(url, cache_days=1)
    assert text
    decoder = json.JSONDecoder()
    chunks: list[str] = []
    offset = text.find(FLIGHT_PUSH)
    while offset != -1:
        start = offset + len(FLIGHT_PUSH)
        offset = text.find(FLIGHT_PUSH, start)
        try:
            pushed, _ = decoder.raw_decode(text, start)
        except json.JSONDecodeError:
            # The framework also pushes runtime values, e.g. `push([0])`.
            continue
        if isinstance(pushed, list) and len(pushed) == 2 and pushed[0] == 1:
            if isinstance(pushed[1], str):
                chunks.append(pushed[1])
    if len(chunks) == 0:
        raise ValueError("No flight document chunks found: %s" % url)
    return "".join(chunks)


def decode_flight_value(document: str, key: str) -> Any:
    """Decode the JSON value stored under `key` in a flight document.

    The document is a stream of framed rows rather than one JSON value, so the
    value is located by its key and consumed from there. Anything other than a
    single occurrence means the page no longer ships the data this crawler reads
    from it, or ships it more than once and the right copy is ambiguous.
    """
    marker = '"%s":' % key
    if document.count(marker) != 1:
        raise ValueError(
            "Flight document has %d %r values" % (document.count(marker), key)
        )
    value, _ = json.JSONDecoder().raw_decode(
        document, document.find(marker) + len(marker)
    )
    return value


def decode_roster(document: str, key: str) -> list[dict[str, Any]]:
    """Decode a roster, checking it against the record count the page reports."""
    roster = decode_flight_value(document, key)
    records: list[dict[str, Any]] = roster["data"]
    reported: int = roster["meta"]["filter_count"]
    if len(records) != reported:
        raise ValueError(
            "Roster %r holds %d of %d records" % (key, len(records), reported)
        )
    return records


def parse_terms(context: Context, options: list[dict[str, Any]]) -> dict[str, Term]:
    """Map each parliamentary session to the Parliament it was part of.

    The dropdown of Parliaments is the only place the source publishes term date
    ranges, and members reference their terms indirectly, by session. The sitting
    Parliament, listed last, is the only one published without a range; any other
    Parliament missing one would make its members look like they never left.
    """
    sessions: dict[str, Term] = {}
    for index, option in enumerate(options):
        title = option.pop("title")
        content = option.pop("content")
        start, end = None, None
        if content is not None:
            match = REGEX_TERM_RANGE.match(squash_spaces(content))
            if match is None:
                raise ValueError("Unexpected %s date range: %r" % (title, content))
            start, end = match.group("start"), match.group("end")
        elif index != len(options) - 1:
            raise ValueError("Parliament without a date range: %r" % title)
        term = Term(title=title, start=start, end=end)
        for session_id in option.pop("parliament_sessions"):
            sessions[session_id] = term
        context.audit_data(option, ignore=["id"])
    return sessions


def parse_name(published_name: str) -> tuple[str, str | None, str | None]:
    """Split a published name into the name and any resignation note it carries.

    Returns the name with the note stripped, and the date a member resigned and
    the Parliament they left where recorded. The note appended to their published
    name is the only place the source records a member leaving early; any other
    annotation is one this crawler cannot read.
    """
    match = REGEX_RESIGNED.search(published_name)
    if match is None:
        if "(" in published_name or ")" in published_name:
            raise ValueError("Unhandled annotation in name: %r" % published_name)
        return published_name, None, None
    return published_name[: match.start()], match.group("date"), match.group("term")


def make_member(context: Context, published_name: str) -> Entity:
    """Create the person for a member, with the name and citizenship both rosters give.

    Members are keyed on their published name: neither roster publishes an
    identifier, and both spell sitting members identically, so the name is what
    makes a member listed in either roster one entity. The honorific titles a name
    carries are a closed set and stripped from the dataset metadata, while the
    "family name, Western given name" ordering some members use is a judgement
    call and left to the name review system.
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
    # A member of Parliament is a citizen of Singapore, whether elected
    # (Constitution of the Republic of Singapore, Article 44(2)(a)) or appointed as
    # a Nominated Member (Fourth Schedule, paragraph 1).
    # https://sso.agc.gov.sg/Act/CONS1963?ProvIds=pr44-
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
    name, resigned_date, resigned_term = parse_name(record.pop("full_name"))
    terms: set[Term] = set()
    for reference in record.pop("parliament_session"):
        session_id = reference["parliament_sessions_id"]
        if session_id not in sessions:
            raise ValueError("Unknown parliamentary session: %r" % session_id)
        # Members are listed once per session, so several references resolve to the
        # same Parliament.
        terms.add(sessions[session_id])
    # The label doubles as a seat type for Nominated and Non-Constituency Members,
    # which is published once per member and so belongs to no single term.
    party = context.lookup("party", record.pop("party_affliation"))
    context.audit_data(
        record,
        ignore=[
            "photo",
            "status",
            # Flags service in the pre-1965 Legislative Assembly, which is outside
            # both this dataset and the PEP relevance window.
            "legislative_assembly",
        ],
    )

    person = make_member(context, name)
    if party is not None:
        person.add("political", party.values)

    occupancies: list[Entity] = []
    for term in terms:
        # The sitting Parliament is published without dates, so its members have
        # none and default to current. The roster is maintained: a member who
        # resigned dropped off the current one within weeks.
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            categorisation=categorisation,
            period_start=term.start,
            period_end=term.end,
            end_date=resigned_date if term.title == resigned_term else None,
        )
        if occupancy is not None:
            occupancies.append(occupancy)
    # Members whose every term is outside the PEP relevance window get no occupancy.
    if len(occupancies) == 0:
        return
    for occupancy in occupancies:
        context.emit(occupancy)
    context.emit(person)


def crawl_sitting_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    record: dict[str, Any],
) -> None:
    """Emit what only the current roster knows about a sitting member.

    The all-Parliaments roster covers the sitting Parliament as well, and both
    rosters produce the same person and the same undated occupancy for it, so this
    adds to those rather than duplicating them.
    """
    person = make_member(context, record["full_name"])
    person.add("sourceUrl", MP_DETAIL_URL % record["url"])
    # Only the current roster publishes a year of birth, and not for every member.
    h.apply_date(person, "birthDate", record["year_of_birth"])

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    # Where the source records the seat type of Nominated and Non-Constituency
    # Members, in place of a constituency.
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
    if "gov.national" not in categorisation.topics:
        # The reviewed topics, not the ones above, decide how long a member stays
        # exposed after leaving office. Without gov.national that is five years
        # rather than twenty, silently reducing this dataset to the two most recent
        # Parliaments.
        context.log.warning(
            "Reviewed position topics don't include gov.national",
            topics=categorisation.topics,
        )
    context.emit(position)

    document = fetch_flight_document(context, context.data_url)
    sessions = parse_terms(context, decode_flight_value(document, "options"))
    for record in decode_roster(document, "mps"):
        crawl_member(context, position, categorisation, record, sessions)

    document = fetch_flight_document(context, CURRENT_MPS_URL)
    for record in decode_roster(document, "initialMPs"):
        crawl_sitting_member(context, position, categorisation, record)
