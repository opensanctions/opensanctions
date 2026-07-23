import re

import pdfplumber
from rigour.mime.types import PDF

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

# The members list is a dated PDF linked from the homepage as
# docs/docs/conseillers<DDMMYYYY>.pdf; the filename date changes on each update.
PDF_LINK_RE = re.compile(r"/docs/docs/conseillers(\d{8})\.pdf$", re.IGNORECASE)

# Names are Arabic-only. The name column sits at table index 3 in the source layout.
NAME_COLUMN = 3
ARABIC_NAME_RE = re.compile(r"^[ء-ي ]+$")
# Non-name rows carried in the same column (section headers, table header).
SKIP_TOKENS = ("الشخصي", "المستشار", "الجهة", "الانتماء", "هيئة", "اللائحة")


def normalize_name(raw: str | None) -> str | None:
    if raw is None:
        return None
    # pdfplumber yields RTL glyphs in reversed visual order; reverse to logical order,
    # drop the tatweel elongation character and collapse whitespace.
    text = raw.replace("\n", " ").strip()[::-1].replace("ـ", "")
    text = " ".join(text.split())
    # A long name wraps so its final letter is split off as a trailing token; rejoin it.
    text = re.sub(r" ([ء-ي])$", r"\1", text)
    return text or None


def find_pdf_url(context: Context) -> str:
    doc = context.fetch_html(context.data_url, headers=HEADERS, cache_days=1)
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
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    pdf_url = find_pdf_url(context)
    path = context.fetch_resource("councillors.pdf", pdf_url, headers=HEADERS)
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    names: set[str] = set()
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables():
                for row in table:
                    if len(row) <= NAME_COLUMN:
                        continue
                    name = normalize_name(row[NAME_COLUMN])
                    if name is None or not ARABIC_NAME_RE.match(name):
                        continue
                    if not 2 <= len(name.split()) <= 5:
                        continue
                    if any(token in name for token in SKIP_TOKENS):
                        continue
                    names.add(name)

    if not names:
        raise ValueError("No councillor names parsed from the members PDF")

    for name in names:
        person = context.make("Person")
        person.id = context.make_id(name)
        person.add("name", name, lang="ara")
        # Eligibility to the House of Councillors requires Moroccan citizenship: only
        # citizens are electors and eligible (2011 Constitution, Article 30).
        # https://mjp.univ-perp.fr/constit/ma2011.htm
        person.add("citizenship", "ma")

        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is None:
            continue
        context.emit(occupancy)
        context.emit(person)
