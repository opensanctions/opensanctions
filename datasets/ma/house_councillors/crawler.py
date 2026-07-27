import re

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

# The PDF is split into one section per electoral college. Each section is
# identified by a stable substring of its title and has its own column layout,
# mapping (name column, party column, region column) by table index; None where
# the column is absent for that college. pdfplumber's column boundaries shift
# between the header and body rows, so the layout is keyed off the section rather
# than the header labels.
SECTIONS: list[tuple[str, tuple[int, int | None, int | None]]] = [
    # 1 - Representatives of regional councils
    ("المجالس الجهو", (3, 0, 4)),
    # 2 - Representatives of communal and prefectural/provincial councils
    ("المجالس الجماع", (3, 0, 4)),
    # 3 - Representatives of professional chambers
    ("الغرف المهن", (3, 0, 4)),
    # 4 - Representatives of employers' organisations (no party affiliation)
    ("المنظمات المهنية للمشغل", (0, None, 3)),
    # 5 - Representatives of employees / trade unions (union affiliation, skipped)
    ("المأجور", (3, None, None)),
]


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


def detect_section(
    table: list[list[str | None]],
) -> tuple[int, int | None, int | None]:
    """Determine the column layout from the section title in the leading rows."""
    header = " ".join(
        text
        for row in table[:3]
        for cell in row
        if (text := reverse_arabic(cell)) is not None
    )
    for keyword, layout in SECTIONS:
        if keyword in header:
            return layout
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

    # (name, party, constituency) per councillor.
    records: list[tuple[str, str | None, str | None]] = []
    current_layout: tuple[int, int | None, int | None] | None = None
    # Region cells are vertically merged: the value appears on the first member
    # of each group and is carried down. Reset when the section changes.
    current_region: str | None = None

    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            if not tables:
                continue
            table = max(tables, key=len)
            layout = detect_section(table)
            if layout != current_layout:
                current_layout = layout
                current_region = None
            name_col, party_col, region_col = layout

            for row in table:
                if len(row) <= name_col:
                    continue
                name = normalize_name(row[name_col])
                if not is_person_name(name):
                    continue
                assert name is not None

                party = None
                if party_col is not None and len(row) > party_col:
                    affiliation = reverse_arabic(row[party_col])
                    if affiliation is not None and affiliation.startswith(PARTY_PREFIX):
                        party = dewrap(affiliation)

                if region_col is not None and len(row) > region_col:
                    region = reverse_arabic(row[region_col])
                    if region is not None:
                        current_region = clean_region(region)
                constituency = current_region if region_col is not None else None

                records.append((name, party, constituency))

    if not records:
        raise ValueError("No councillor names parsed from the members PDF")

    for name, party, constituency in records:
        person = context.make("Person")
        person.id = context.make_id(name, party, constituency)
        person.add("name", name, lang="ara")
        apply_translit_full_name(context, person, LangText(name, "ara"))
        # The party the member ran for; trade-union affiliation (employees'
        # college) is not a political association and is not recorded.
        person.add("political", party, lang="ara")
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
        occupancy.add("constituency", constituency, lang="ara")
        context.emit(occupancy)
        context.emit(person)
