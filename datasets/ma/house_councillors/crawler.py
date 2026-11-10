import re
from typing import NamedTuple

import pdfplumber
from rigour.mime.types import PDF

from zavod import Context
from zavod import helpers as h
from zavod.shed.trans import apply_translit_full_name
from zavod.stateful.positions import categorise
from zavod.util import LangText

# The members list is a dated PDF linked from the homepage as
# docs/docs/conseillers<DDMMYYYY>.pdf; the filename date changes on each update.
PDF_LINK_RE = re.compile(r"/docs/docs/conseillers(\d{8})\.pdf$", re.IGNORECASE)

TATWEEL = "ـ"
# A person name is Arabic letters and spaces only, 2-5 words.
ARABIC_NAME_RE = re.compile(r"^[ء-ي ]+$")
# Party names are prefixed with "party of" in the affiliation column.
PARTY_PREFIX = "حزب"
# Some names carry a trailing footnote reference number (e.g. two members share a
# name); it is not part of the name.
FOOTNOTE_RE = re.compile(r"\s*\d+$")
# A long value wraps so its final letter is split off as a trailing token.
TRAILING_LETTER_RE = re.compile(r" ([ء-ي])$")
# Non-name tokens carried in the name column (table/section header labels).
SKIP_TOKENS = ("الشخصي", "المستشار", "الجهة", "الدائرة", "الانتماء", "هيئة", "اللائحة")


class RowCells(NamedTuple):
    """The raw cell texts of one table row, None where the section has no column."""

    name: str | None
    party: str | None
    region: str | None


class Section(NamedTuple):
    """One section of the PDF, which lists the members of one electoral college.

    pdfplumber's column boundaries shift between the header and body rows, so the
    column layout is keyed off the section rather than the header labels.
    """

    keyword: str
    """A stable substring of the section title."""
    name_col: int
    party_col: int | None
    region_col: int | None

    def read(self, row: list[str | None]) -> RowCells:
        def cell(col: int | None) -> str | None:
            if col is None or col >= len(row):
                return None
            return row[col]

        return RowCells(
            cell(self.name_col), cell(self.party_col), cell(self.region_col)
        )


SECTIONS = [
    # 1 - Representatives of regional councils
    Section("المجالس الجهو", name_col=3, party_col=0, region_col=4),
    # 2 - Representatives of communal and prefectural/provincial councils
    Section("المجالس الجماع", name_col=3, party_col=0, region_col=4),
    # 3 - Representatives of professional chambers
    Section("الغرف المهن", name_col=3, party_col=0, region_col=4),
    # 4 - Representatives of employers' organisations (no party affiliation)
    Section("المنظمات المهنية للمشغل", name_col=0, party_col=None, region_col=3),
    # 5 - Representatives of employees / trade unions (union affiliation, skipped)
    Section("المأجور", name_col=3, party_col=None, region_col=None),
]


class Cell(NamedTuple):
    raw: str
    """The cell text as pdfplumber extracts it."""
    text: str
    """The cleaned text in logical order."""


class CouncillorRecord(NamedTuple):
    name: Cell
    party: Cell | None
    constituency: Cell | None


def reverse_arabic(raw: str | None) -> str | None:
    """Turn pdfplumber's reversed visual glyph order into logical order.

    Each line is reversed independently and lines are kept in their original
    top-to-bottom order, so multi-line cells (compound electoral districts)
    reconstruct correctly. The tatweel elongation character is dropped and
    whitespace collapsed.
    """
    if raw is None:
        return None
    lines = [line.strip()[::-1].replace(TATWEEL, "") for line in raw.split("\n")]
    text = " ".join(line for line in lines if line)
    text = " ".join(text.split())
    return text or None


def dewrap(text: str) -> str:
    """Rejoin a final letter that wrapped onto its own trailing token."""
    return TRAILING_LETTER_RE.sub(r"\1", text)


def clean_region(text: str) -> str:
    """Normalise a region / electoral district string.

    The source renders the separators between region parts inconsistently (plain
    hyphen vs en-dash, with varying spacing); unify them so the same region reads
    identically everywhere.
    """
    text = dewrap(text).replace("–", "-").replace("—", "-")
    text = re.sub(r"\s*([/-])\s*", r" \1 ", text)
    return " ".join(text.split())


def normalize_name(raw: str | None) -> str | None:
    text = reverse_arabic(raw)
    if text is None:
        return None
    text = FOOTNOTE_RE.sub("", text).strip()
    return dewrap(text) or None


def is_person_name(name: str | None) -> bool:
    if name is None:
        return False
    if not ARABIC_NAME_RE.match(name):
        return False
    if name.startswith(PARTY_PREFIX):
        return False
    if not 2 <= len(name.split()) <= 5:
        return False
    return not any(token in name for token in SKIP_TOKENS)


def detect_section(table: list[list[str | None]]) -> Section:
    """Determine the section from its title in the leading rows."""
    header = " ".join(
        text
        for row in table[:3]
        for cell in row
        if (text := reverse_arabic(cell)) is not None
    )
    for section in SECTIONS:
        if section.keyword in header:
            return section
    raise ValueError(f"Unrecognised section header: {header!r}")


def find_pdf_url(context: Context) -> str:
    doc = context.fetch_html(context.data_url, cache_days=14)
    candidates: list[tuple[str, str]] = []
    for href in h.xpath_strings(doc, "//a/@href"):
        match = PDF_LINK_RE.search(href)
        if match is not None:
            candidates.append((match.group(1), href))
    if not candidates:
        raise ValueError("Could not find the councillors list PDF on the homepage")
    # Pick the most recent by the DDMMYYYY date embedded in the filename.
    _, url = max(candidates, key=lambda c: (c[0][4:], c[0][2:4], c[0][:2]))
    if url.startswith("/"):
        url = "https://www.chambredesconseillers.ma" + url
    return url


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the House of Councillors of Morocco",
        country="ma",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21328580",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    pdf_url = find_pdf_url(context)
    path = context.fetch_resource("councillors.pdf", pdf_url)
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    records: list[CouncillorRecord] = []
    current_section: Section | None = None
    # Region cells are vertically merged: the value appears on the first member
    # of each group and is carried down. Reset when the section changes.
    current_region: Cell | None = None

    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            # pdfplumber gives a ligature glyph, such as lam-alef, as several
            # characters in logical order. Reverse them so that they read
            # correctly after reverse_arabic turns the visual order around.
            for char in page.chars:
                if len(char["text"]) > 1:
                    char["text"] = char["text"][::-1]
            tables = page.extract_tables()
            if not tables:
                continue
            table = max(tables, key=len)
            section = detect_section(table)
            if section != current_section:
                current_section = section
                current_region = None

            for row in table:
                cells = section.read(row)
                name = normalize_name(cells.name)
                if not is_person_name(name):
                    continue
                assert cells.name is not None and name is not None

                party = None
                affiliation = reverse_arabic(cells.party)
                if (
                    cells.party is not None
                    and affiliation is not None
                    and affiliation.startswith(PARTY_PREFIX)
                ):
                    party = Cell(cells.party, dewrap(affiliation))

                region = reverse_arabic(cells.region)
                if cells.region is not None and region is not None:
                    current_region = Cell(cells.region, clean_region(region))
                constituency = None if section.region_col is None else current_region

                records.append(
                    CouncillorRecord(Cell(cells.name, name), party, constituency)
                )

    if not records:
        raise ValueError("No councillor names parsed from the members PDF")

    for record in records:
        party = record.party
        constituency = record.constituency
        person = context.make("Person")
        # Key the ID on the raw cell texts, so that changes to the text cleaning
        # don't change the IDs.
        person.id = context.make_id(
            record.name.raw,
            None if party is None else party.raw,
            None if constituency is None else constituency.raw,
        )
        person.add("name", record.name.text, lang="ara")
        apply_translit_full_name(context, person, LangText(record.name.text, "ara"))
        # The party the member ran for; trade-union affiliation (employees'
        # college) is not a political association and is not recorded.
        if party is not None:
            person.add("political", party.text, lang="ara")
        # Eligibility to the House of Councillors requires Moroccan citizenship: only
        # citizens are electors and eligible (2011 Constitution, Article 30).
        # https://mjp.univ-perp.fr/constit/ma2011.htm
        person.add("citizenship", "ma")

        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is None:
            continue
        # The region or electoral district the member represents.
        if constituency is not None:
            occupancy.add("constituency", constituency.text, lang="ara")
        context.emit(occupancy)
        context.emit(person)
