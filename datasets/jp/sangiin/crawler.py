import re
import unicodedata
from urllib.parse import urljoin

from lxml import html
from requests import HTTPError

from zavod import Context, Entity
from zavod import helpers as h
from zavod import settings
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

# The /current/ redirect stub links to the active Diet-session roster, e.g.
# /japanese/joho1/kousei/giin/221/giin.htm — capture the session number from it.
SESSION_RE = re.compile(r"/giin/(\d+)/giin\.htm")
PROFILE_RE = re.compile(r"profile/(\d+)\.htm")
SESSION_URL = "https://www.sangiin.go.jp/japanese/joho1/kousei/giin/%d/giin.htm"
# The site only keeps recent sessions online; descending from the latest until a 404
# walks the whole live window. The cap is a runaway backstop, not an expected bound.
MAX_SESSIONS = 30

# Japanese era (和暦) → Gregorian: year = base + era-year (era-year 1 may be written 元).
ERA_BASE = {"令和": 2018, "平成": 1988, "昭和": 1925, "大正": 1911, "明治": 1867}
# Term-end dates (任期満了) on the roster are always full era dates, e.g. 令和10年7月25日.
ERA_DATE_RE = re.compile(r"(令和|平成|昭和|大正|明治)(元|\d+)年(\d+)月(\d+)日")
# Profiles open with the birth date — either an era date or a plain Gregorian year, with
# or without month/day — so match the date itself rather than relying on a fixed suffix.
DOB_RE = re.compile(r"^\s*(令和|平成|昭和|大正|明治)?(\d+|元)年(?:(\d+)月(\d+)日)?")
# The birthplace, when cleanly stated, follows the date up to a birth marker (生まれ/生).
# Bounded to one clause; profiles that bury it in prose are left without a birthplace.
BIRTHPLACE_RE = re.compile(r"^[、\s]*([^。、]{1,30}?)(?:に|にて|で)?(?:生まれ|生)")


def era_to_iso(text: str) -> str | None:
    """Convert the first full Japanese era date in `text` (e.g. 令和10年7月25日) to ISO."""
    m = ERA_DATE_RE.search(unicodedata.normalize("NFKC", text))
    if m is None:
        return None
    era, year, month, day = m.groups()
    gregorian = ERA_BASE[era] + (1 if year == "元" else int(year))
    return f"{gregorian:04d}-{int(month):02d}-{int(day):02d}"


def parse_birth(text: str) -> tuple[str | None, str | None]:
    """Extract (birthDate, birthPlace) from a profile's lead paragraph.

    Profiles open with the member's birth date; an optional birthplace clause follows
    (e.g. `昭和40年8月18日東京都墨田区生まれ。`, `1956年仙台市生まれ。`). The date is
    captured to whatever precision is given (year, or full date); the birthplace is
    best-effort and omitted when not cleanly delimited. Returns (None, None) when the
    paragraph does not start with a recognisable date.
    """
    norm = unicodedata.normalize("NFKC", text)
    m = DOB_RE.match(norm)
    if m is None:
        return None, None
    era, year, month, day = m.groups()
    gregorian = ERA_BASE[era] + (1 if year == "元" else int(year)) if era else int(year)
    if gregorian < 1900 or gregorian > 2010:
        return None, None  # leading number was not a plausible birth year
    date = f"{gregorian:04d}"
    if month is not None and day is not None:
        date = f"{date}-{int(month):02d}-{int(day):02d}"
    place_match = BIRTHPLACE_RE.match(norm[m.end() :])
    place = place_match.group(1).strip() if place_match is not None else None
    return date, (place or None)


def fetch_doc(context: Context, url: str, cache_days: int) -> Element:
    """Fetch and parse an HTML page, decoding it as the UTF-8 the page declares.

    The site sends no charset in its HTTP headers, so `fetch_text` falls back to
    ISO-8859-1 and mangles the Japanese. ISO-8859-1 is byte-preserving, so re-encoding
    recovers the raw bytes and lets lxml honour the UTF-8 `<meta>` charset.
    """
    text = context.fetch_text(url, cache_days=cache_days)
    if text is None or len(text) == 0:
        raise ValueError("Empty document: %s" % url)
    return html.fromstring(text.encode("latin-1"))


def session_rows(context: Context, session: int) -> list[Element] | None:
    """Return the member rows of one session roster, or None if the session is 404.

    A 404 marks the lower edge of the rolling window of online sessions.
    """
    try:
        doc = fetch_doc(context, SESSION_URL % session, cache_days=1)
    except HTTPError as err:
        if err.response is not None and err.response.status_code == 404:
            return None
        raise
    return h.xpath_elements(doc, ".//tr[.//a[contains(@href, 'profile/')]]")


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    member: dict[str, str | bool],
) -> None:
    person = context.make("Person")
    person.id = context.make_slug(str(member["profile_id"]))
    h.apply_reviewed_names(
        context,
        person,
        original=h.Names(name=str(member["name"])),
        llm_cleaning=True,
        lang="jpn",
    )
    person.add("alias", member["reading"])  # kana reading (読み方)
    person.add("sourceUrl", member["profile_url"])
    # Public Offices Election Act (公職選挙法) Art. 10 requires Japanese nationality for
    # eligibility to the House of Councillors: https://laws.e-gov.go.jp/law/325AC1000000100/
    person.add("citizenship", "jp")

    # Profile pages carry DOB/birthplace but 404 once a member has left the chamber.
    try:
        prof = fetch_doc(context, str(member["profile_url"]), cache_days=7)
        para = h.xpath_elements(prof, ".//p[@class='profile2']")
        if para:
            birth_date, birth_place = parse_birth(h.element_text(para[0]))
            h.apply_date(person, "birthDate", birth_date)
            person.add("birthPlace", birth_place)
    except HTTPError as err:
        if err.response is None or err.response.status_code != 404:
            raise
        context.log.info("No profile page (departed member)", id=member["profile_id"])

    # Status: current members carry a future term-end (任期満了) → `current`. A departed
    # member whose term-end is already past simply reached the end of their term (→ `ended`);
    # one whose term-end is still in the future left mid-term, so the scheduled date is not a
    # real end date and is dropped (→ `unknown`).
    end_iso = era_to_iso(str(member["term_end"]))
    today = settings.RUN_TIME.date().isoformat()
    # The scheduled term-end is a reliable end date only when the member still sits in the
    # latest session or the date is already past; a departed member with a future term-end
    # left mid-term, so we drop the date and let status fall to `unknown`.
    term_end_reliable = bool(member["in_latest"]) or (
        end_iso is not None and end_iso < today
    )
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        end_date=end_iso if term_end_reliable else None,
        no_end_implies_current=term_end_reliable,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("politicalGroup", member["faction"])  # 会派, in-chamber faction
    occupancy.add("constituency", member["district"])  # 選挙区
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the House of Councillors of Japan",
        wikidata_id="Q14552828",
        country="jp",
        topics=["gov.legislative", "gov.national"],
    )
    categorisation = categorise(context, position)
    context.emit(position)

    # Discover the active session from the /current/ redirect stub.
    landing = fetch_doc(context, context.data_url, cache_days=1)
    anchor = h.xpath_element(landing, ".//a[contains(@href, '/giin/')]")
    session_match = SESSION_RE.search(anchor.get("href") or "")
    assert session_match is not None, anchor.get("href")
    latest = int(session_match.group(1))

    # Walk the rolling window newest-first; union members keyed on the stable profile id,
    # so each person keeps their most recent faction/constituency/term-end and we record
    # whether they still sit in the latest session.
    members: dict[str, dict[str, str | bool]] = {}
    for session in range(latest, latest - MAX_SESSIONS, -1):
        rows = session_rows(context, session)
        if rows is None:
            break
        if session == latest:
            assert len(rows) > 200, len(rows)
        for row in rows:
            cells = h.xpath_elements(row, "./td")
            assert len(cells) >= 5, len(cells)
            link = h.xpath_element(cells[0], ".//a")
            profile_url = urljoin(SESSION_URL % session, link.get("href"))
            profile_match = PROFILE_RE.search(profile_url)
            assert profile_match is not None, profile_url
            profile_id = profile_match.group(1)
            if profile_id in members:
                continue  # already captured from a more recent session
            members[profile_id] = {
                "profile_id": profile_id,
                "profile_url": profile_url,
                "in_latest": session == latest,
                "name": h.element_text(link),
                "reading": h.element_text(cells[1]),
                "faction": h.element_text(cells[2]),
                "district": h.element_text(cells[3]),
                "term_end": h.element_text(cells[4]),
            }
    assert members, "no sessions found"

    for member in members.values():
        crawl_member(context, position, categorisation, member)
