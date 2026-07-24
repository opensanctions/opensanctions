from html import unescape

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise


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
        if term not in starts or onboard < starts[term]:
            starts[term] = onboard
    return starts


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
    onboard = row.pop("onboardDate")

    # End date: an early departure (leaveDate) takes precedence; otherwise the member
    # served to the end of the term, which is the start of the following term; the most
    # recent term has no successor yet, so its sitting members have no end date.
    leave_flag = row.pop("leaveFlag")
    leave_date = row.pop("leaveDate") or None
    if leave_flag not in ("否", "是"):
        raise ValueError(f"Unexpected leaveFlag: {leave_flag!r}")
    end_date: str | None
    if leave_flag == "是":
        if leave_date is None:
            raise ValueError(f"Departed member without leaveDate: {name!r}")
        end_date = leave_date
    else:
        end_date = next_start[term]

    person = context.make("Person")
    person.id = context.make_id(term, name, onboard)

    h.apply_reviewed_names(
        context,
        person,
        original=h.Names(name=name),
        llm_cleaning=True,
        lang="zho",
    )

    person.add("name", row.pop("ename"), lang="eng")
    person.add("gender", row.pop("sex"))
    # The free-text degree field uses angle brackets for annotations (e.g.
    # 學士<鋼琴演奏>); normalise them to CJK parentheses so they aren't flagged as markup.
    degree = row.pop("degree")
    if degree is not None:
        degree = degree.replace("<", "（").replace(">", "）")
    person.add("education", degree)
    # Members of a national legislature must be ROC nationals: the Civil Servants Election
    # and Recall Act (公職人員選舉罷免法) limits suffrage to ROC citizens (Art. 14) and
    # requires candidates to be electors (Art. 24).
    # https://law.moj.gov.tw/ENG/LawClass/LawAll.aspx?pcode=D0020010
    person.add("citizenship", "tw")

    party = row.pop("party")
    party_res = context.lookup("party", party)
    person.add("political", party_res.value if party_res else party, lang="zho")

    political_group = row.pop("partyGroup")
    political_group_res = context.lookup("party", political_group)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=onboard,
        end_date=end_date,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add(
        "politicalGroup",
        political_group_res.value if political_group_res else political_group,
        lang="zho",
    )
    occupancy.add("constituency", row.pop("areaName"))

    context.emit(occupancy)
    context.emit(person)

    context.audit_data(
        row,
        ignore=[
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
    # The server (behind a uproxy) intermittently sends a `Content-Encoding: gzip`
    # header with a body that isn't valid gzip, breaking the client decompressor. Opt
    # out of gzip so it returns the response as plain `text/plain`.
    data = context.fetch_json(
        context.data_url,
        cache_days=1,
        headers={"Accept-Encoding": "identity"},
    )
    rows: list[dict[str, str]] = [r for r in data["dataList"] if r.get("name")]

    starts = term_starts(rows)
    ordered = sorted(starts, key=int)
    next_start: dict[str, str | None] = {
        term: starts[ordered[i + 1]] if i + 1 < len(ordered) else None
        for i, term in enumerate(ordered)
    }
    position = h.make_position(
        context,
        name="Member of the Legislative Yuan",
        country="tw",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q6310593",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    for row in rows:
        # `cutoff` is ISO (dash-separated); normalise the source separator to compare.
        if starts[row["term"]].replace("/", "-") < h.earliest_term_start(
            position.get("topics")
        ):
            continue
        crawl_member(context, row, starts, next_start, position, categorisation)
