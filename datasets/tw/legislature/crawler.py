import re
from html import unescape

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

DATE_RE = re.compile(r"^\d{4}/\d{2}/\d{2}$")


def check_date(value: str) -> str:
    """Assert the source `YYYY/MM/DD` date shape and return it unchanged.

    The source uses a single date format throughout; fail loudly if that ever changes so
    we don't silently misorder terms or mis-bound occupancies. The raw value is returned
    so that `apply_date` records it as the statement's original value; the lexicographic
    order of `YYYY/MM/DD` strings matches chronological order, so they sort correctly."""
    if not DATE_RE.match(value):
        raise ValueError("Unexpected date format: %r" % value)
    return value


def term_starts(rows: list[dict[str, str]]) -> dict[str, str]:
    """Map each term id to its start date (raw `YYYY/MM/DD`).

    Legislative Yuan terms are contiguous, so the start of one term is the end of the
    previous one. Each term's start is the earliest onboarding date among its members
    (consistently 1 February of the election year), which lets us derive term-end dates
    from the data instead of hardcoding a constitutional date table."""
    starts: dict[str, str] = {}
    for row in rows:
        term = row["term"]
        onboard = row.get("onboardDate")
        if not onboard:
            continue
        onboard = check_date(onboard)
        if term not in starts or onboard < starts[term]:
            starts[term] = onboard
    return starts


def add_political(
    context: Context, entity: Entity, prop: str, value: str | None
) -> None:
    """Add a party affiliation / parliamentary caucus name as published (Chinese).

    The `party` lookup only maps non-affiliation markers (independent, a data glitch) to
    null so they are dropped; every other value is kept verbatim — names are not
    translated."""
    if not value:
        return
    result = context.lookup("party", value)
    if result is not None and result.value is None:
        return
    entity.add(prop, value, lang="zho")


def crawl_member(
    context: Context,
    row: dict[str, str],
    starts: dict[str, str],
    next_start: dict[str, str | None],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    # The source occasionally HTML-escapes characters (e.g. &#183; for a middle dot).
    row = {k: unescape(v) if isinstance(v, str) else v for k, v in row.items()}
    term = row.pop("term")
    name = row.pop("name")
    onboard = check_date(row.pop("onboardDate"))

    # End date: an early departure (leaveDate) takes precedence; otherwise the member
    # served to the end of the term, which is the start of the following term; the most
    # recent term has no successor yet, so its sitting members have no end date.
    leave_flag = row.pop("leaveFlag")
    leave_date = row.pop("leaveDate") or None
    if leave_flag not in ("否", "是"):
        raise ValueError("Unexpected leaveFlag: %r" % leave_flag)
    end_date: str | None
    if leave_flag == "是":
        if leave_date is None:
            raise ValueError("Departed member without leaveDate: %r" % name)
        end_date = check_date(leave_date)
    else:
        end_date = next_start[term]

    person = context.make("Person")
    # No stable per-person id is published (picUrl is missing for older terms and is
    # term-prefixed where present), so key on the term plus name; the onboarding date
    # disambiguates a hypothetical same-name collision within a term. The same person
    # across terms becomes several entities, merged downstream by the resolver.
    person.id = context.make_id(term, name, onboard)
    person.add("name", name)
    person.add("name", row.pop("ename"), lang="eng")  # romanised; kept verbatim
    person.add("gender", row.pop("sex"))  # via type.gender lookup
    # The free-text degree field uses angle brackets for annotations (e.g.
    # 學士<鋼琴演奏>); normalise them to CJK parentheses so they aren't flagged as markup.
    degree = row.pop("degree") or None
    if degree is not None:
        degree = degree.replace("<", "（").replace(">", "）")
    person.add("education", degree)
    # Members of a national legislature must be ROC nationals: the Civil Servants Election
    # and Recall Act (公職人員選舉罷免法) limits suffrage to ROC citizens (Art. 14) and
    # requires candidates to be electors (Art. 24).
    # https://law.moj.gov.tw/ENG/LawClass/LawAll.aspx?pcode=D0020010
    person.add("citizenship", "tw")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=onboard,
        end_date=end_date,
        categorisation=categorisation,
    )
    if occupancy is None:
        # Party affiliation doesn't affect PEP status, so it is set only for members we
        # actually emit — avoids spurious "unmapped party" warnings for dropped terms.
        return

    add_political(context, person, "political", row.pop("party") or None)
    add_political(context, occupancy, "politicalGroup", row.pop("partyGroup") or None)

    context.emit(occupancy)
    context.emit(person)

    context.audit_data(
        row,
        ignore=[
            "areaName",
            "committee",
            "tel",
            "fax",
            "addr",
            "experience",
            "leaveReason",
            "picUrl",
        ],
    )


def crawl(context: Context) -> None:
    # Dataset ID 16 ("歷屆委員資料", all-terms legislators). Without a `term` filter it
    # returns the full historical roster, so the crawler stays current across new terms
    # without pinning a term number. (ID 9, "當屆委員資料", only ever returns the current
    # term and ignores the `term` filter.)
    data = context.fetch_json(context.data_url, cache_days=1)
    rows: list[dict[str, str]] = [r for r in data["dataList"] if r.get("name")]

    starts = term_starts(rows)
    ordered = sorted(starts, key=int)
    next_start: dict[str, str | None] = {
        term: starts[ordered[i + 1]] if i + 1 < len(ordered) else None
        for i, term in enumerate(ordered)
    }

    # Only consider terms recent enough to still be in the PEP look-back window; the
    # precise per-member end-date cut-off is then applied by make_occupancy.
    cutoff = h.earliest_term_start(["gov.national"])

    position = h.make_position(
        context,
        name="Member of the Legislative Yuan",
        country="tw",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q6310593",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    context.emit(position)

    for row in rows:
        # `cutoff` is ISO (dash-separated); normalise the source separator to compare.
        if starts[row["term"]].replace("/", "-") < cutoff:
            continue
        crawl_member(context, row, starts, next_start, position, categorisation)
