import re
from urllib.parse import urljoin

from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

from zavod import Context
from zavod import helpers as h

# Roster URLs vary, so discover them by legislature number in the link text. The site
# uses the "th" suffix for every ordinal, including "1th" and "2th".
LEGISLATURE_LINK = re.compile(r"members?'?s? of (\d+)th legislature", re.IGNORECASE)

# The parliament does not publish term dates. These come from Wikipedia's articles on
# each general election; add the next date before crawling a new legislature.
ELECTION_DATES = {
    6: "1998-03-06",
    7: "2002-05-02",
    8: "2004-07-06",
    9: "2008-09-02",
    10: "2012-10-30",
    11: "2016-01-22",
    12: "2020-03-19",
    13: "2022-10-13",
    14: "2025-01-16",
}

# Column labels vary by legislature; unknown or duplicate normalized columns fail.
COLUMNS = {
    "name": "name",
    "constituency": "constituency",
    "home_islandaddress": "home_island",
    "party": "party",
    "affiliating_party": "party",
    "position_portfolio": "portfolio",
    "position": "portfolio",
    "profile": "profile",
}

# The 7th legislature embeds the constituency in its home-island field, sometimes
# without spaces between words.
HOME_ISLAND_SEAT = re.compile(
    r"\(\s*Parliament\s*Member\s*for\s*(.+?)\s*Constituency\s*\)",
    re.IGNORECASE | re.DOTALL,
)

# Departure notes follow the name after an inconsistently spaced hyphen. Requiring
# whitespace on one side preserves hyphenated names.
DEPARTURE_NOTE = re.compile(r"\s+-\s*|\s*-\s+")


def normalise_columns(
    legislature: int, row: dict[str, str | None]
) -> dict[str, str | None]:
    normalised: dict[str, str | None] = {}
    for label, value in row.items():
        column = COLUMNS.get(label)
        assert column is not None, (legislature, label)
        assert column not in normalised, (legislature, column)
        normalised[column] = value
    return normalised


def detach_departed_rows(table: Element) -> list[Element]:
    """Detach narrower rows appended for members who left mid-term.

    The first width mismatch also removes the empty footer on the 13th legislature page.
    """
    rows = h.xpath_elements(table, ".//tr")
    if not rows:
        return []
    columns = len(h.xpath_elements(rows[0], "./th|./td"))
    for index, row in enumerate(rows[1:], start=1):
        if len(h.xpath_elements(row, "./td")) == columns:
            continue
        departed = rows[index:]
        for detached in departed:
            parent = detached.getparent()
            assert parent is not None
            parent.remove(detached)
        return departed
    return []


def crawl_departed(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    legislature: int,
    rows: list[Element],
) -> int:
    members = 0
    for row in rows:
        cells = [h.element_text(cell) for cell in h.xpath_elements(row, "./th|./td")]
        # Section headings do not start with the roster's "Hon." honorific.
        if not cells or not cells[0].startswith("Hon"):
            continue
        parts = DEPARTURE_NOTE.split(cells[0], maxsplit=1)
        assert len(parts) == 2, (legislature, cells[0])
        member: dict[str, str | None] = {"name": parts[0], "notes": parts[1]}
        # Only the bribery section repeats party and constituency.
        if len(cells) == 3:
            member["party"] = cells[1] or None
            member["constituency"] = cells[2] or None
        crawl_member(context, position, categorisation, legislature, member)
        members += 1
    return members


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    legislature: int,
    row: dict[str, str | None],
) -> None:
    raw_name = row.pop("name")
    assert raw_name is not None, (legislature, row)
    name = h.strip_name_titles(context, raw_name)
    assert name, (legislature, raw_name)

    constituency = row.pop("constituency", None)
    home_island = row.pop("home_island", None)
    if home_island is not None:
        seat = HOME_ISLAND_SEAT.search(home_island)
        if seat is not None:
            constituency = seat.group(1)
        else:
            # Some cells hold text other than an address, e.g. a ministerial portfolio.
            result = context.lookup("home_island_seat", home_island)
            if result is None:
                context.log.warning(
                    "No constituency in home island cell",
                    legislature=legislature,
                    name=name,
                    home_island=home_island,
                )
            else:
                constituency = result.value

    person = context.make("Person")
    person.id = context.make_id(name, constituency)
    person.add("name", name, original_value=raw_name if name != raw_name else None)
    person.add("political", row.pop("party", None))
    person.add("notes", row.pop("notes", None))
    person.add("citizenship", "vu")

    # Individual appointment dates are unavailable, so use the legislature's bounds.
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        period_start=ELECTION_DATES[legislature],
        period_end=ELECTION_DATES.get(legislature + 1),
        categorisation=categorisation,
    )
    # Old terms can fall outside the position's after-office period.
    if occupancy is None:
        return
    occupancy.add("constituency", constituency)

    context.audit_data(row, ignore=["portfolio", "profile"])
    context.emit(occupancy)
    context.emit(person)


def crawl_legislature(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    legislature: int,
    url: str,
) -> int:
    doc = zyte_api.fetch_html(
        context,
        url,
        unblock_validator=".//table",
        geolocation="au",
        cache_days=14,
    )
    members = 0
    # Older pages split ministers and backbenchers across tables.
    for table in h.xpath_elements(doc, ".//table"):
        departed = detach_departed_rows(table)
        for row in h.parse_html_table(table):
            cells = normalise_columns(legislature, h.cells_to_str(row))
            crawl_member(context, position, categorisation, legislature, cells)
            members += 1
        members += crawl_departed(
            context, position, categorisation, legislature, departed
        )
    return members


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Parliament of Vanuatu",
        country="vu",
        wikidata_id="Q21294920",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    index = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=".//a[contains(@href, 'legislature')]",
        geolocation="au",
        cache_days=14,
    )
    crawled: set[int] = set()
    for link in h.xpath_elements(index, ".//a[@href]"):
        label = LEGISLATURE_LINK.search(h.element_text(link))
        if label is None:
            continue
        legislature = int(label.group(1))
        # The navigation repeats itself across the desktop and mobile menus.
        if legislature in crawled:
            continue
        assert legislature <= max(ELECTION_DATES), (
            f"Legislature {legislature} has no election date on record. Add it to "
            "ELECTION_DATES so its term can be dated."
        )
        if legislature not in ELECTION_DATES:
            context.log.info("Legislature predates the PEP cutoff", n=legislature)
            continue
        crawled.add(legislature)

        href = link.get("href")
        assert href is not None
        members = crawl_legislature(
            context,
            position,
            categorisation,
            legislature,
            urljoin(context.data_url, href),
        )
        # Fewer rows indicate that the page structure changed.
        assert members >= 45, (legislature, members)

    missing = set(ELECTION_DATES) - crawled
    assert not missing, f"Legislatures missing from the navigation: {sorted(missing)}"
