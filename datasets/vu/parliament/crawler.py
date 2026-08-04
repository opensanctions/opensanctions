import re
from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

# A roster page exists per legislature, reachable only from the site navigation, and
# their URLs don't share a shape: the 13th sits at .../legislatures/members-of-parliament
# while the 14th sits outside the .../legislatures/ path the older ones use. The ordinal
# in the link label is the one stable handle, so match on that and follow wherever it
# points. Every label uses the "th" suffix, including "1th" and "2th".
LEGISLATURE_LINK = re.compile(r"members?'?s? of (\d+)th legislature", re.I)

# General election dates, which the parliament doesn't publish alongside the rosters.
# A legislature runs from its own election until the next one, so only the start is
# recorded and the sitting legislature is the one with no successor here. Adding the
# next election is what brings a newly elected legislature into the dataset.
#
# Terms are cut off at 1998 because members of anything older are past the after-office
# period for a national legislator and no longer count as PEPs. Dates are taken from the
# Wikipedia article on each Vanuatuan general election; the parliament's own pages carry
# no term dates at all.
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

# Column labels drift between legislatures: the 7th heads its columns "Home
# Island/Address" and "Affiliating Party", the 13th and 14th add a "Profile" link.
# Mapping every known label onto one shape lets a single parser read every page, and an
# unmapped label stops the crawl rather than quietly dropping a column.
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

# The 7th legislature gives a home island and address in place of a constituency, naming
# the constituency in a trailing parenthetical. Line breaks in the source run words
# together ("Parliament Memberfor"), hence the tolerance for missing spaces.
HOME_ISLAND_SEAT = re.compile(
    r"\(\s*Parliament\s*Member\s*for\s*(.+?)\s*Constituency\s*\)", re.I | re.S
)

# Members who left partway through a term are written as "Hon. Jerry Kanas -Deceased on
# the 24th June 2019 ...", with the hyphen spaced inconsistently. Requiring whitespace on
# one side of it keeps a hyphenated name from being read as a departure note.
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


def split_departed(table: Element) -> list[Element]:
    """Detach and return the rows listing members who left partway through the term.

    The 10th and 11th legislature pages append sections headed "Decesased" (sic),
    "Convicted for bribery" and "Electrol Petition" (sic) below the roster. These are
    members in their own right — none of them are repeated in the roster above, having
    been struck from it when they left — but they use a narrower set of columns, so the
    first row that doesn't match the header width marks where the roster ends.

    Measuring `td` rather than `th|td` also sheds the row of five empty header cells the
    13th legislature's table ends with, which `parse_html_table` would otherwise read as
    a member row with no cells in it.
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
        # Section headings carry a label rather than a member; every member is written
        # with the "Hon." honorific the roster uses.
        if not cells or not cells[0].startswith("Hon"):
            continue
        parts = DEPARTURE_NOTE.split(cells[0], maxsplit=1)
        assert len(parts) == 2, (legislature, cells[0])
        member: dict[str, str | None] = {"name": parts[0], "notes": parts[1]}
        # Only the bribery section repeats the party and constituency columns.
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
    # Names carry the honorific "Hon." (declared under `names.prefixes_strip`) and use
    # non-breaking spaces, which strip_name_titles normalises. The recent pages write the
    # surname in upper case; we keep the source casing since the matcher normalises it.
    name = h.strip_name_titles(context, raw_name)
    assert name, (legislature, raw_name)

    constituency = row.pop("constituency", None)
    home_island = row.pop("home_island", None)
    if home_island is not None:
        seat = HOME_ISLAND_SEAT.search(home_island)
        if seat is None:
            context.log.warning(
                "No constituency in home island cell",
                legislature=legislature,
                name=name,
                home_island=home_island,
            )
        else:
            constituency = seat.group(1)

    person = context.make("Person")
    person.id = context.make_id(name, constituency)
    person.add("name", name, original_value=raw_name if name != raw_name else None)
    person.add("political", row.pop("party", None))
    person.add("notes", row.pop("notes", None))
    # Every citizen of Vanuatu at least 25 years of age is eligible to stand for
    # Parliament (Constitution of Vanuatu, Chapter 4, Article 17(2)).
    # https://www.constituteproject.org/constitution/Vanuatu_2013
    person.add("citizenship", "vu")

    # The term is dated, the individual tenure isn't: a member can be elected at a
    # by-election or leave early, so periodStart/periodEnd rather than startDate/endDate.
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        period_start=ELECTION_DATES[legislature],
        period_end=ELECTION_DATES.get(legislature + 1),
        categorisation=categorisation,
    )
    # Sitting in a legislature that rose longer ago than the after-office period doesn't
    # make someone a PEP today, and make_occupancy returns None for those terms.
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
        # A roster table must be present for the fetch to count as unblocked.
        unblock_validator=".//table",
        geolocation="au",
        cache_days=14,
    )
    members = 0
    # Ministers and backbenchers are listed in two separate tables on several of the
    # older pages, so take every table on the page rather than only the first.
    for table in h.xpath_elements(doc, ".//table"):
        departed = split_departed(table)
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
        # The navigation carrying the per-legislature links must have rendered.
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
        # Parliament has had at least 46 seats since 1998, so a page yielding a handful
        # of rows means a table was restructured rather than a term being short-handed.
        assert members >= 45, (legislature, members)

    missing = set(ELECTION_DATES) - crawled
    assert not missing, f"Legislatures missing from the navigation: {sorted(missing)}"
