"""Parse consolidated Regulation (EC) 765/2006 (Belarus) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation is a hybrid: one designation annex among many sectoral
goods annexes. The entity-listing annexes:

- Annex I — the Article 2(1) fund-freeze list, parts A. Natural persons
  and B. Legal persons, entities and bodies, each one six-column table
  (entry number, Latin transliterations, Belarusian/Russian spellings,
  identifying information, reasons, date of listing). Travel bans live in
  Decision 2012/642/CFSP, not in this regulation.
- Annex V — persons, entities and bodies under Articles 1e(7), 1f(7) and
  1fa (dual-use and advanced-technology restrictions), a plain name list.
- Annex IX — major credit institutions under Articles 1j and 1k, a plain
  name list.
- Annex XV — legal persons, entities and bodies excluded from specialised
  financial messaging under Article 1zb, a name/date table.
- Annex XXXIV — crypto-assets and central bank digital currencies under
  Article 1ze, a name/date table.
- Annex XXXIII (Article 1zd) is currently published empty.

All other annexes list goods, technology, software, partner countries,
authority websites, or authorisation forms — no designations.

In Annex I part A the name cell prints the Belarusian and Russian
transliterations as separate lines and the native cell both spellings; the
first Latin line is the name and every other rendering an alias. A few
entries continue into a second table row whose number and name cells are
empty; the continuation's identifying information and reasons belong to
the preceding entry. Delisted entries leave numbering gaps. Relational
"Associated …" lines name other parties, have no CSV column, and are
deliberately not transcribed. Dates are transcribed as the source prints
them ("21.6.2021", "20 March 2022"); the crawler normalizes dates.

Output: data/consolidated/32006R0765.csv (the EU Journal consolidated CSV
contract, keyed by the framework act). The consolidated version the snapshot
was extracted from is passed as the CELEX argument and pinned in the dataset
YAML's `config.consolidation`, updated in the same commit as the CSV.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import click
from common import (
    LABELLED_RE,
    SKIP_P_CLASSES,
    ParseError,
    Row,
    annex_blocks,
    annex_id,
    cell_line,
    cell_lines,
    check_consolidated_celex,
    check_marker,
    check_registry,
    clean,
    load_source,
    split_values,
    summary,
    table_body,
    to_record,
    validate_records,
    verbatim_date,
    write_csv,
)
from lxml import html
from zavod.helpers.html import element_text, xpath_elements
from zavod.util import Element

FRAMEWORK_CELEX = "32006R0765"
PROGRAM_KEY = "EU-BLR"

# Annex I implements the Article 2(1) fund freeze; travel bans live in
# Decision 2012/642/CFSP.
ANNEX_I_MEASURE = "Asset freeze"
ANNEX_I_SUBTITLE = (
    "List of natural and legal persons, entities and bodies referred to in Article 2(1)"
)
ANNEX_I_HEADER = (
    "",
    "Names (Transliteration of Belarusian spelling) "
    "(Transliteration of Russian spelling)",
    "Names (Belarusian spelling) (Russian spelling)",
    "Identifying information",
    "Reasons for listing",
    "Date of listing",
)
# (part heading, part id, schema) in print order.
ANNEX_I_PARTS = (
    ("A. Natural persons referred to in Article 2(1)", "A", "Person"),
    (
        "B. Legal persons, entities and bodies referred to in Article 2(1)",
        "B",
        "LegalEntity",
    ),
)

# The other entity-listing annexes: plain p.norm name lists and two-column
# name/date tables. (schema, measure, header or None).
NORM_LIST_TARGETS = {
    "V": ("LegalEntity", "Export control"),
    "IX": ("LegalEntity", "Financial restrictions"),
}
NAME_DATE_TARGETS = {
    "XV": (
        "LegalEntity",
        "Financial restrictions",
        ("Name of the legal person, entity or body", "Date of application"),
    ),
    "XXXIV": (
        "Asset",
        "Financial restrictions",
        ("Crypto-assets or central bank digital currencies", "Entry into force"),
    ),
}
EXPECTED_EMPTY = frozenset({"XXXIII"})
NON_TARGET = frozenset(
    {
        "II",
        "III",
        "IV",
        "Va",
        "Vb",
        "Vba",
        "Vc",
        "VI",
        "VII",
        "VIII",
        "X",
        "XI",
        "XII",
        "XIII",
        "XIV",
        "XIVa",
        "XVI",
        "XVII",
        "XVIII",
        "XIX",
        "XX",
        "XXI",
        "XXII",
        "XXIII",
        "XXIV",
        "XXV",
        "XXVI",
        "XXVII",
        "XXVIII",
        "XXIX",
        "XXX",
        "XXXI",
        "XXXII",
    }
)

NUMBER_RE = re.compile(r"^(\d+)\.$")
# Alias labels in name cells: as a line prefix ("a.k.a. BelOil"), inline
# after a name, and as a trailing parenthetical ("MZKT (a.k.a. VOLAT)").
AKA_RE = re.compile(r"^a\.k\.a\.?:? (.+)$")
INLINE_AKA_RE = re.compile(r"^(.+?) a\.k\.a\.?:? (.+)$")
NAME_PAREN_TAIL_RE = re.compile(r"^(.+) \(a\.k\.a\.?:? (.+)\)$")

# Part B entries listing unlabeled variant names as bare lines under the
# name; the printed list structure marks them as aliases. (Part A never
# needs pins: its header promises transliteration lines, all aliases.)
ALIAS_LINE_PINS = frozenset(
    {("B", "12"), ("B", "13"), ("B", "22"), ("B", "39"), ("B", "47"), ("B", "56")}
)
# One native cell wraps its second rendering across two lines (given name /
# patronymic+surname); default is one variant rendering per line.
NATIVE_WRAP_PINS = frozenset({("A", "156")})

# Reviewed hand-mappings for name-cell lines that are not names, keyed by
# (part, entry) and the exact line. If the source line changes, the lookup
# misses and the run breaks for re-review.
NAME_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    # The printed second line states the legal form, not a name.
    ("B", "16"): {
        "State-owned enterprise": (("legalForm", "State-owned enterprise"),),
    },
    # The listing explicitly extends to the entity's own branch.
    ("B", "25"): {
        "Including Branch ‘Khimvolokno Plant’ JSC ‘Grodno Azot’": (
            ("notes", "Including Branch ‘Khimvolokno Plant’ JSC ‘Grodno Azot’"),
        ),
    },
}
NATIVE_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    ("B", "16"): {
        "Дзяржаўнае прадпрыемства": (("legalForm", "Дзяржаўнае прадпрыемства"),),
        "Государственное предприятие": (("legalForm", "Государственное предприятие"),),
    },
    ("B", "25"): {
        "Фiлiял ‘Завод Хiмвалакно’ ААТ ‘Гродна Азот’": (
            ("notes", "Фiлiял ‘Завод Хiмвалакно’ ААТ ‘Гродна Азот’"),
        ),
        "Филиал ‘Завод Химволокно’ ОАО ‘Гродно Азот’": (
            ("notes", "Филиал ‘Завод Химволокно’ ОАО ‘Гродно Азот’"),
        ),
    },
    # The Chinese rendering is printed as an image with an OCR caption; the
    # annotation lines are markup, not names.
    ("B", "61"): {
        "Chinese:": (),
        "Text of image": (),
        "中国航天三江集团有限公司": (("alias", "中国航天三江集团有限公司"),),
    },
}

# Identifying-information labels → CSV column, exactly as printed.
INFO_LABELS = {
    "DOB": "birthDate",
    "POB": "birthPlace",
    "Gender": "gender",
    "Nationality": "nationality",
    "Position(s)": "position",
    "Position": "position",
    "Function": "position",
    "Rank": "position",
    "Address": "address",
    "Suspected address": "address",
    "Suspected location": "address",
    # Mostly street addresses and cities — a place, not a jurisdiction.
    "Place of registration": "address",
    "Principal place of business": "address",
    "Passport number": "passportNumber",
    "Passport": "passportNumber",
    "Passport no.": "passportNumber",
    "BY passport number": "passportNumber",
    "Belarusian passport number": "passportNumber",
    "Russian passport number": "passportNumber",
    "Personal ID": "idNumber",
    # One entry prints a space before the colon; another truncates the label.
    "Personal ID ": "idNumber",
    "Personal": "idNumber",
    "Personal identification": "idNumber",
    "National ID": "idNumber",
    "Belarusian ID": "idNumber",
    "Tax identification number": "taxNumber",
    "Registration number": "registrationNumber",
    "Registration number (УНН/ИНН)": "registrationNumber",
    "OKPO": "okpoCode",
    "Date of registration": "incorporationDate",
    "Date of Registration": "incorporationDate",
    "Date of establishement": "incorporationDate",
    "Website": "website",
    "Websites": "website",
    "Web": "website",
    "Company website": "website",
    "Email": "email",
    "Email address": "email",
    "E-mail": "email",
    "E-mail address": "email",
    "E-Mail": "email",
    "e-mail": "email",
    "Company email": "email",
    "Phone": "phone",
    "Phone number": "phone",
    "Tel.": "phone",
    "Tel. (office)": "phone",
    "Tel./Fax": "phone",
    "Fax": "phone",
    "Company phone": "phone",
    "Type of entity": "legalForm",
    # A printed former-name label ("Maiden name: Kirsanova … or Selyun …");
    # the value stays whole, the crawler's review system categorises it.
    "Maiden name": "previousName",
}
# Columns whose labelled value legitimately continues onto bare follow-on
# lines in this document (positions spanning paragraphs, address lists).
CONTINUABLE_COLUMNS = frozenset({"position", "address"})
# Relational labels naming other parties: no CSV column, deliberately not
# transcribed.
DROP_LABELS = frozenset({"Associated entities", "Associated individuals"})
# Reviewed hand-mappings for identifying-information lines the label rules
# cannot place, keyed by (part, entry) and the exact line. If the source
# line changes, the lookup misses and the run breaks for re-review.
INFO_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    # A passport line printed without its colon, in a continuation row.
    ("A", "113"): {"Passport MP2156098": (("passportNumber", "MP2156098"),)},
    # The Russian ИНН/КПП pair wraps mid-token onto the next line.
    ("B", "5"): {
        "Registration number (УНН/ИНН): 190950894 (Belarus); 7704734000/": (
            ("registrationNumber", "190950894 (Belarus)"),
        ),
        "770301001 (Russia).": (
            ("registrationNumber", "7704734000/770301001 (Russia)"),
        ),
        "Tel. +375 (17) 240-36-50": (("phone", "+375 (17) 240-36-50"),),
    },
    ("B", "13"): {
        "Tel. + 375 (17) 217-22-22; + 8000 217-22-22": (
            ("phone", "+ 375 (17) 217-22-22"),
            ("phone", "+ 8000 217-22-22"),
        ),
    },
    ("B", "29"): {
        "Tel. +375 (17) 309-30-10; +375 (17) 309-30-30": (
            ("phone", "+375 (17) 309-30-10"),
            ("phone", "+375 (17) 309-30-30"),
        ),
    },
    # A registration history: one dated renaming per printed line, with
    # trailing list commas dropped.
    ("B", "37"): {
        "Date of registration: 24.4.1991 as ‘БЕЛОРУССКИЙ МЕТАЛЛУРГИЧЕСКИЙ ЗАВОД’,": (
            ("incorporationDate", "24.4.1991 as ‘БЕЛОРУССКИЙ МЕТАЛЛУРГИЧЕСКИЙ ЗАВОД’"),
        ),
        "11.9.1996 as ‘Государственное предприятие – Белорусский металлургический завод’,": (
            (
                "incorporationDate",
                "11.9.1996 as ‘Государственное предприятие – Белорусский металлургический завод’",
            ),
        ),
        "1.12.1997 as ‘Белорусский металлургический завод’,": (
            ("incorporationDate", "1.12.1997 as ‘Белорусский металлургический завод’"),
        ),
        "3.11.1999 as ‘Республиканское унитарное предприятие ‘Белорусский металлургический завод’’,": (
            (
                "incorporationDate",
                "3.11.1999 as ‘Республиканское унитарное предприятие ‘Белорусский металлургический завод’’",
            ),
        ),
        "1.1.2012 as ‘Открытое акционерное общество ‘Белорусский металлургический завод’’": (
            (
                "incorporationDate",
                "1.1.2012 as ‘Открытое акционерное общество ‘Белорусский металлургический завод’’",
            ),
        ),
    },
    ("B", "39"): {
        "Tel. + 375296615929;": (("phone", "+ 375296615929"),),
        "+375172006232": (("phone", "+375172006232"),),
    },
    ("B", "43"): {
        "Phone number + 375173292103": (("phone", "+ 375173292103"),),
    },
    ("B", "54"): {"info@vistan.ru": (("email", "info@vistan.ru"),)},
    ("B", "55"): {
        "https://lasercut.by/": (("website", "https://lasercut.by/"),),
        "+375 17 390 30 76": (("phone", "+375 17 390 30 76"),),
    },
}
# Listing dates printed with a stray trailing period ("3.6.2022.",
# "24 May 2026."); the period is list punctuation, not date wording.
DATE_PERIOD_PINS = frozenset({("I.B", "28"), ("XXXIV", "")})


# Only the dotted and full-month forms occur in this document.
DATE_FORMATS = (
    "dotted",
    "worded",
)


def apply_override(row: Row, mapped: tuple[tuple[str, str], ...]) -> None:
    for column, value in mapped:
        row.add(column, [value])


def parse_latin_names(
    ctx: str, part: str, record_id: str, td: Element, row: Row
) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    first = lines[0]
    tail = NAME_PAREN_TAIL_RE.match(first)
    if tail is not None:
        row.add("alias", [tail.group(2)])
        first = tail.group(1)
    inline = INLINE_AKA_RE.match(first)
    if inline is not None:
        row.add("alias", [inline.group(2)])
        first = inline.group(1)
    row.add("name", [first])
    overrides = NAME_OVERRIDES.get((part, record_id), {})
    for line in lines[1:]:
        if line in overrides:
            apply_override(row, overrides[line])
            continue
        aka = AKA_RE.match(line)
        if aka is not None:
            row.add("alias", [aka.group(1)])
            continue
        # Part A prints the Belarusian and Russian transliterations as
        # separate unlabeled lines; the header marks them all as renderings
        # of the name. Part B variant lines are pinned per entry.
        if part == "A" or (part, record_id) in ALIAS_LINE_PINS:
            row.add("alias", [line])
            continue
        raise ParseError(f"{ctx}: unrecognized name line {line[:60]!r}")


def parse_native_names(
    ctx: str, part: str, record_id: str, td: Element, row: Row
) -> None:
    # The Belarusian/Russian spellings are aliases; the first Latin
    # transliteration holds the primary name. Cells are legitimately empty.
    lines = cell_lines(td, ctx)
    if (part, record_id) in NATIVE_WRAP_PINS and len(lines) > 1:
        lines = [lines[0], " ".join(lines[1:])]
    overrides = NATIVE_OVERRIDES.get((part, record_id), {})
    for line in lines:
        if line in overrides:
            apply_override(row, overrides[line])
            continue
        inline = INLINE_AKA_RE.match(line)
        if inline is not None:
            row.add("alias", [inline.group(1), inline.group(2)])
            continue
        aka = AKA_RE.match(line)
        if aka is not None:
            row.add("alias", [aka.group(1)])
            continue
        row.add("alias", [line])


def parse_info(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    overrides = INFO_OVERRIDES.get((part, record_id), {})
    # Part A cells open with unlabeled position prose before the first
    # labelled line. A labelled line then opens a block; bare lines continue
    # the block for the columns the document has shown continuations for
    # (positions spanning paragraphs, address lists), extend dropped
    # relational content, or — for a value ending in "," — wrap mid-phrase.
    leading = True
    block: str | None = None
    opened_empty = False
    dropped = False
    wrapped: str | None = None
    for line in lines:
        if line in overrides:
            apply_override(row, overrides[line])
            leading, block, opened_empty, dropped, wrapped = (
                False,
                None,
                False,
                False,
                None,
            )
            continue
        labelled = LABELLED_RE.match(line)
        label = labelled.group(1) if labelled is not None else None
        if label in DROP_LABELS:
            leading, block, opened_empty, dropped, wrapped = (
                False,
                None,
                False,
                True,
                None,
            )
            continue
        if label is not None and label in INFO_LABELS:
            assert labelled is not None
            column = INFO_LABELS[label]
            value = labelled.group(2)
            if column == "previousName":
                # The maiden-name value stays whole; it is one printed name.
                row.add(column, [value])
            elif value != "":
                row.add(column, split_values(value))
            # An empty-valued label ("Tel.:") holds its value on the
            # following bare lines, and a value ending in ";" continues its
            # list there; a value ending in "," wraps mid-phrase.
            leading, block, dropped = False, column, False
            opened_empty = value == "" or value.endswith(";")
            wrapped = column if value.endswith(",") else None
            continue
        if dropped:
            continue
        if wrapped is not None:
            merged = f"{row.props[wrapped][-1]} {line}"
            row.props[wrapped][-1] = merged
            wrapped = wrapped if merged.endswith(",") else None
            continue
        if leading and part == "A":
            # Unlabeled position prose at the top of part A cells.
            row.add("position", [line])
            continue
        if block is not None and (opened_empty or block in CONTINUABLE_COLUMNS):
            row.add(block, split_values(line))
            continue
        raise ParseError(f"{ctx}: unrecognized info line {line[:60]!r}")


def continue_row(ctx: str, part: str, row: Row, cells: list[Element]) -> None:
    """Merge a continuation row (empty number cell) into the previous entry."""
    ctx = f"{ctx} entry {row.record_id} (cont.)"
    for index in (1, 2):
        if cell_lines(cells[index], ctx):
            raise ParseError(f"{ctx}: unexpected name content in continuation row")
    info_lines = cell_lines(cells[3], ctx)
    parse_info(ctx, part, row.record_id, cells[3], row)
    reason_lines = cell_lines(cells[4], ctx)
    if not info_lines and not reason_lines:
        raise ParseError(f"{ctx}: continuation row without content")
    if reason_lines:
        row.reason = " ".join([row.reason, *reason_lines])
    if cell_lines(cells[5], ctx):
        raise ParseError(f"{ctx}: unexpected date in continuation row")


def parse_annex_i_row(roman: str, part: str, schema: str, tr: Element) -> Row:
    cells = xpath_elements(tr, "./td|./th")
    ctx = f"{roman}.{part}"
    number = NUMBER_RE.match(cell_line(cells[0], ctx))
    if number is None:
        raise ParseError(f"{ctx}: unrecognized entry number cell")
    record_id = number.group(1)
    ctx = f"{ctx} entry {record_id}"
    annex = annex_id(roman, part)
    row = Row(annex, schema, ANNEX_I_MEASURE, record_id=record_id)
    parse_latin_names(ctx, part, record_id, cells[1], row)
    parse_native_names(ctx, part, record_id, cells[2], row)
    parse_info(ctx, part, record_id, cells[3], row)
    row.reason = " ".join(cell_lines(cells[4], ctx))
    date_text = cell_line(cells[5], ctx)
    if (annex, record_id) in DATE_PERIOD_PINS:
        date_text = date_text.removesuffix(".")
    row.start_date = verbatim_date(date_text, ctx, DATE_FORMATS)
    return row


def parse_annex_i(roman: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    headings: list[str] = []
    tables: list[Element] = []
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls == "modref":
            check_marker(" ".join(element_text(child).split()), roman)
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            continue
        if child.tag == "p" and cls == "norm":
            if clean(element_text(child), roman) != ANNEX_I_SUBTITLE:
                raise ParseError(f"{roman}: unexpected annex prose")
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-1":
            headings.append(clean(element_text(child), roman))
            continue
        if child.tag == "div" and cls == "centered":
            tables.append(xpath_elements(child, ".//table", expect_exactly=1)[0])
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    expected = [heading for heading, _, _ in ANNEX_I_PARTS]
    if headings != expected or len(tables) != len(ANNEX_I_PARTS):
        raise ParseError(f"{roman}: part structure {headings} != {expected}")
    for (_, part, schema), table in zip(ANNEX_I_PARTS, tables, strict=True):
        ctx = f"{roman}.{part}"
        part_rows: list[Row] = []
        for tr in table_body(ctx, table, ANNEX_I_HEADER):
            cells = xpath_elements(tr, "./td|./th")
            if not cell_lines(cells[0], ctx):
                if not part_rows:
                    raise ParseError(f"{ctx}: continuation before first entry")
                continue_row(ctx, part, part_rows[-1], cells)
                continue
            part_rows.append(parse_annex_i_row(roman, part, schema, tr))
        if not part_rows:
            raise ParseError(f"{ctx}: no entries extracted")
        rows.extend(part_rows)
    return rows


def iter_norm_lines(roman: str, block: Element) -> list[str]:
    """Collect the non-empty p.norm entry lines of a plain-list annex."""
    lines: list[str] = []
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls == "modref":
            check_marker(" ".join(element_text(child).split()), roman)
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            continue
        if child.tag == "p" and cls == "norm":
            text = clean(element_text(child), roman)
            if text:
                lines.append(text)
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    return lines


def parse_norm_list(roman: str, schema: str, measure: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    for name in iter_norm_lines(roman, block):
        row = Row(roman, schema, measure)
        row.add("name", [name])
        rows.append(row)
    if not rows:
        raise ParseError(f"{roman}: no entries extracted")
    return rows


def parse_name_date_table(
    roman: str, schema: str, measure: str, header: tuple[str, ...], block: Element
) -> list[Row]:
    rows: list[Row] = []
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls == "modref":
            check_marker(" ".join(element_text(child).split()), roman)
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            continue
        if child.tag == "div" and cls == "centered":
            table = xpath_elements(child, ".//table", expect_exactly=1)[0]
            for tr in table_body(roman, table, header):
                cells = xpath_elements(tr, "./td|./th")
                row = Row(roman, schema, measure)
                row.add("name", [cell_line(cells[0], roman)])
                date_text = cell_line(cells[1], roman)
                if (roman, "") in DATE_PERIOD_PINS:
                    date_text = date_text.removesuffix(".")
                row.start_date = verbatim_date(date_text, roman, DATE_FORMATS)
                rows.append(row)
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if not rows:
        raise ParseError(f"{roman}: no entries extracted")
    return rows


def assert_no_entries(roman: str, block: Element) -> None:
    if xpath_elements(block, ".//table | .//div"):
        raise ParseError(f"{roman}: expected-empty annex has entry content")
    if iter_norm_lines(roman, block):
        raise ParseError(f"{roman}: expected-empty annex has entry lines")


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    known = (
        {"I"}
        | set(NORM_LIST_TARGETS)
        | set(NAME_DATE_TARGETS)
        | EXPECTED_EMPTY
        | NON_TARGET
    )
    for roman, block in annex_blocks(doc, known):
        if roman in NON_TARGET:
            continue
        if roman in EXPECTED_EMPTY:
            assert_no_entries(roman, block)
            continue
        if roman == "I":
            rows.extend(parse_annex_i(roman, block))
        elif roman in NORM_LIST_TARGETS:
            schema, measure = NORM_LIST_TARGETS[roman]
            rows.extend(parse_norm_list(roman, schema, measure, block))
        else:
            schema, measure, header = NAME_DATE_TARGETS[roman]
            rows.extend(parse_name_date_table(roman, schema, measure, header, block))
    return rows


@click.command(help="Parse consolidated Regulation 765/2006 into a CSV candidate.")
@click.argument("celex")
@click.option(
    "--source",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Parse these exact XHTML bytes instead of fetching from CELLAR.",
)
def main(celex: str, source: Path | None) -> None:
    try:
        check_registry(
            PROGRAM_KEY,
            [
                ANNEX_I_MEASURE,
                *(measure for _, measure in NORM_LIST_TARGETS.values()),
                *(measure for _, measure, _ in NAME_DATE_TARGETS.values()),
            ],
            [
                *(schema for _, _, schema in ANNEX_I_PARTS),
                *(schema for schema, _ in NORM_LIST_TARGETS.values()),
                *(schema for schema, _, _ in NAME_DATE_TARGETS.values()),
            ],
        )
        check_consolidated_celex(celex, FRAMEWORK_CELEX)
        content = load_source(celex, source)
        doc = html.fromstring(content)
        rows = parse_document(doc)
        records = [to_record(row, FRAMEWORK_CELEX, PROGRAM_KEY) for row in rows]
        validate_records(records)
        csv_path = write_csv(records, FRAMEWORK_CELEX)
        click.echo(json.dumps(summary(records, celex), indent=2))
        click.echo(f"wrote {csv_path}")
    except ParseError as exc:
        raise click.ClickException(str(exc)) from exc


if __name__ == "__main__":
    main()
