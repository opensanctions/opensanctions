"""Parse consolidated Regulation (EC) 881/2002 (ISIL/Al-Qaida) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation transposes the UN Security Council ISIL (Da'esh) and
Al-Qaida sanctions list. Its annexes:

- Annex I — the Article 2 fund-freeze list, printed not as tables but as
  one flowing paragraph per entry under two section headings ("Legal
  persons, groups and entities", then "Natural persons"). Entries carry no
  numbers, so recordId stays empty. Two markup generations coexist: bare
  ``div.list`` paragraphs and dash-bulleted ``div.grid-container`` items.
- Annex IA — a single natural person referred to in Article 2(3a), one
  ``p.norm`` paragraph.
- Annex II — competent-authority websites, not designations.

Each entry paragraph is UN-formatted prose: a name (with parenthesized
alias groups and, in newer entries, sentence-level "Good quality a.k.a.:"
lists) followed by "Label: value" sentences and a final "Date of
designation referred to in Article …:" date. Labels are matched only at
sentence boundaries and outside parentheses; a known label glued to the
previous value by a printing defect must be pinned before it splits.
Native-script renderings are usually embedded images and cannot be
transcribed (dropped); two old entries print them as text (aliases).
A handful of pre-2002 legacy entries in OFAC/Taliban-era shorthand do not
follow the grammar at all and are transcribed by hand, keyed on their
exact printed text. The section note "(functions in brackets are those
under the former Taliban regime of Afghanistan)" makes the bare
parenthetical of those legacy person entries a function (position).
Relational "Associated with …" prose lives inside "Other information"
values and is kept there verbatim. Dates are transcribed as the source
prints them ("6.10.2001", "21 Oct. 2013"); the crawler normalizes dates.

Output: data/consolidated/32002R0881.csv (the EU Journal consolidated CSV
contract, keyed by the framework act). The consolidated version the snapshot
was extracted from is passed as the CELEX argument and pinned in the dataset
YAML's `consolidation` lookup, updated in the same commit as the CSV.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import get_args

import click
from common import (
    ParseError,
    Row,
    annex_blocks,
    bare_text,
    check_marker,
    clean,
    load_source,
    parse_abbrev_date,
    parse_dotted_date,
    summary,
    to_record,
    validate_records,
    write_csv,
)
from followthemoney import model
from lxml import html
from zavod.helpers.html import element_text, xpath_elements
from zavod.stateful.programs import Measure, get_program_by_key
from zavod.util import Element

FRAMEWORK_CELEX = "32002R0881"
CONSOLIDATED_RE = re.compile(r"^02002R0881-\d{8}$")
PROGRAM_KEY = "EU-TAQA-EUAQ"
# The regulation implements the fund freeze; travel bans ride on the UN
# regime and Decision (CFSP) 2016/1693.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"II"})

ANNEX_I_TITLE = "List of persons, groups and entities referred to in Article 2"
# (section heading, schema) in print order.
SECTIONS = (
    ("Legal persons, groups and entities", "LegalEntity"),
    ("Natural persons", "Person"),
)
# Printed directly under the "Natural persons" heading.
SECTION_NOTE = (
    "(functions in brackets are those under the former Taliban regime of Afghanistan)"
)
ANNEX_IA_TITLE = "Natural person referred to in Article 2(3a)"

# The per-entry designation-date labels, exactly as printed (nine observed
# punctuation variants). The date is always the entry's final sentence.
DESIG_LABELS = (
    "Date of designation referred to in Article 2a (4) (b):",
    "Date of designation referred to in Article 2a(4)(b):",
    "Date of designation referred to in Article 2a(4) (b):",
    "Date of designation referred to in Article 2a (4)(b):",
    "Date of designation referred to in Article 2a(4), point (b):",
    "Date of designation referred to in Article 7d(2)(i):",
    "Date of designation referred to in Article 7d(2), point (i):",
    "Date of designation referred to in Article 7e(e):",
    "Date of Designation: referred to in Article 2a (4) (b):",
)

# Sentence-level "Label:" → CSV column, exactly as printed (including the
# lowercase spellings of a few 1990s-era entries). Labels are recognized
# only at a sentence boundary (start, ". " or "; ") at parenthesis depth 0.
# An empty column is a deliberate drop: kin names have no CSV column per
# the contract (fatherName excepted).
INFO_LABELS = {
    "Good quality a.k.a.": "alias",
    "Low quality a.k.a.": "weakAlias",
    "A.k.a.": "alias",
    "F.k.a.": "previousName",
    "Date of birth": "birthDate",
    "date of birth": "birthDate",
    "Place of birth": "birthPlace",
    "Place of Birth": "birthPlace",
    "place of birth": "birthPlace",
    "Nationality": "nationality",
    "nationality": "nationality",
    "Address": "address",
    "Previous address": "address",
    "Passport No": "passportNumber",
    "Passport No.": "passportNumber",
    "Passport no": "passportNumber",
    "Passport no.": "passportNumber",
    "passport No": "passportNumber",
    "Passport number": "passportNumber",
    "Passports": "passportNumber",
    "National identification No": "idNumber",
    "National identification No.": "idNumber",
    "National identification no": "idNumber",
    "National identification no.": "idNumber",
    "National Identification No": "idNumber",
    "National identification number": "idNumber",
    "national identification": "idNumber",
    "Identity card No": "idNumber",
    "Ration card number": "idNumber",
    "Italian fiscal code": "taxNumber",
    "Italian Fiscal Code": "taxNumber",
    "Title": "position",
    "Function": "position",
    "Gender": "gender",
    "Sex": "gender",
    "Website": "website",
    "Phone number": "phone",
    "Other information": "notes",
    "other information": "notes",
    "Physical description": "appearance",
    "Distinguishing marks": "appearance",
    "Profession": "position",
    "Ethnic background": "ethnicity",
    "Father’s name": "fatherName",
    "Father's name": "fatherName",
    "Name of father": "fatherName",
    "Mother’s name": "",
    "Mother's name": "",
    "Wife’s name": "",
    "Husband's name is": "",
    "Grandfather’s name": "",
}
# Columns whose values are alias-style lists, split on the printed
# enumeration.
NAME_LIST_COLUMNS = frozenset({"alias", "weakAlias", "previousName"})
# Printed placeholder values, dropped per the contract.
PLACEHOLDER_VALUES = frozenset({"na"})

_LABEL_ALT = "|".join(
    re.escape(label) for label in sorted(INFO_LABELS, key=len, reverse=True)
)
LABEL_AT_RE = re.compile(r"(?:" + _LABEL_ALT + r"):\s*")
# A 1990s-era style terminates two labels with their abbreviation dot and
# no colon ("Passport no. L335915"); the value must open with a passport-
# or identity-number token.
DOTLBL_AT_RE = re.compile(
    r"(?:Passport no\.|National identification no\.) (?=[A-Z0-9])"
)
DESIG_AT_RE = re.compile(
    r"(?:" + "|".join(re.escape(label) for label in DESIG_LABELS) + r")\s*"
)

# Labels checked for inside extracted values: a known label left in a
# value marks a missed split (a printing defect to pin) or an embedded
# item to route. "Other information" itself is excluded — it only ever
# opens a segment.
_RESIDUE_LABELS = [label for label, column in INFO_LABELS.items() if column != "notes"]
RESIDUE_RE = re.compile(
    r"(?:"
    + "|".join(
        re.escape(label) for label in sorted(_RESIDUE_LABELS, key=len, reverse=True)
    )
    + r"):\s*"
)

# Alias-list sublabels inside the name region's parentheticals; a group
# may chain several, separated by "; " ("(good quality alias: X; low
# quality alias: Y)").
GROUP_SUBLABEL_RE = re.compile(
    r"(?:[Gg]ood quality alias|[Ll]ow quality alias|alias|aka"
    r"|original script|script original|formerly listed as):?\s*"
)
GROUP_SUBLABEL_COLUMNS = {
    "good quality alias": "alias",
    "alias": "alias",
    "aka": "alias",
    "low quality alias": "weakAlias",
    "original script": "alias",
    "script original": "alias",
    "formerly listed as": "previousName",
}
# The native-script sublabels; an empty value is an embedded image the
# parser cannot transcribe and the sublabel is dropped.
SCRIPT_SUBLABELS = frozenset({"original script", "script original"})

# Legacy person entries whose bare parenthetical is a function under the
# former Taliban regime, per the printed section note. Keyed by the lead
# name (text before the parenthetical).
FUNCTION_PAREN_LEADS: frozenset[str] = frozenset(
    {
        "Hassan, Hadji Mohammad, Mullah",
    }
)

# A printed defect gluing a known label to the previous value without a
# sentence boundary; each pin is the exact glued text and the offset of the
# label within it, reviewed against the source.
GLUED_LABEL_PINS: dict[str, int] = {
    # Zawahiri: "Title: (a) Doctor, (b) Dr Date of birth: 19.6.1951."
    "Dr Date of birth:": 3,
    # Al-Fadhli: "Nationality: Kuwait Passport no: (a) …"
    "Kuwait Passport no:": 7,
    # Sahiron: "Address: Sulu region, Philippines (reported location) Date
    # of birth: (a) 1955 …"
    "(reported location) Date of birth:": 20,
    # Gunawan: "Other information: Brother of Nurjaman Riduan Isamuddin
    # Date of designation …"
    "Isamuddin Date of designation referred to in Article 2a (4) (b):": 10,
    # Al-Juburi: "Low quality a.k.a.: … d) Abu Umar Title: Amir."
    "Abu Umar Title:": 9,
    # Maychou: "Passport no: Morocco number V06359364 National
    # identification no: …"
    "V06359364 National identification no:": 10,
    # Nabaggala: "National identification no.: CF89095102DDAE (expired on
    # 27.3.2025) Address: …"
    "(expired on 27.3.2025) Address:": 23,
    # Muthana: "… expires on 27 Jul. 2020) Other information: …"
    "27 Jul. 2020) Other information:": 14,
}

# Reviewed structural repairs for printing defects (a lost parenthesis, a
# stray quote, an enumeration letter missing its bracket). Each pin is the
# exact printed text and its repaired form; values never change. A source
# edit breaks the key and resurfaces the original parse error.
MISPRINT_REPAIRS: dict[str, str] = {
    # Global Relief Foundation: missing sentence stop before "Address:".
    "Foundation (GRF) Address:": "Foundation (GRF). Address:",
    # Harakat Ul-Mujahidin: alias (g) printed outside the closed group.
    "(f) Harakat Ul-Mujahideen), (g) HUM.": "(f) Harakat Ul-Mujahideen, (g) HUM).",
    # Special Purpose Islamic Regiment: stray quote from the amending act.
    "(d) SPIR).’ Other information:": "(d) SPIR). Other information:",
    # Ansar Al Charia Benghazi: alias group closed early at item (d).
    "(d) Ansar al-Charia Benghazi); (e)": "(d) Ansar al-Charia Benghazi; (e)",
    # Mujahidin Indonesian Timur: missing sentence stop before "Address:".
    "of Western Indonesia) Address:": "of Western Indonesia). Address:",
    # Al-Aouadi: sentence stop printed inside the alias group.
    "(b) Fathi Hannachi.) Date of birth:": "(b) Fathi Hannachi). Date of birth:",
    # Al-Maaroufi: "Address" printed without its colon.
    "Al Djoundoubi). Address (a) rue": "Al Djoundoubi). Address: (a) rue",
    # Abdel Rahman: missing sentence stop before "Good quality a.k.a.:".
    "عبد الرحمن) Good quality a.k.a.:": "عبد الرحمن). Good quality a.k.a.:",
    # Al Ghabra: missing sentence stop before "Address:".
    "(b) Danial Adam) Address:": "(b) Danial Adam). Address:",
    # Al-Qaduli: the space after the alias-list separator is missing.
    ";low quality alias:": "; low quality alias:",
    # Al Furqan: a stray closing parenthesis ends the address list.
    "Zavidovici, Bosnia and Herzegovina). Other information:": (
        "Zavidovici, Bosnia and Herzegovina. Other information:"
    ),
    # Al Zahrani: "Date of birth" printed without its colon.
    "al-Khozmri). Date of birth 15.9.1978.": "al-Khozmri). Date of birth: 15.9.1978.",
    # Al-Kawari: "Date of birth" printed without its colon.
    "Abu Ali al-Kawari). Date of birth 28.9.1973.": (
        "Abu Ali al-Kawari). Date of birth: 28.9.1973."
    ),
    # Al-Juburi: a stray pipe before the designation date.
    "Subhah Muhammad Sayf. | Date of designation": (
        "Subhah Muhammad Sayf. Date of designation"
    ),
    # Atabiev: missing sentence stop before "Date of birth:".
    "(alias Abu Jihad) Date of birth:": "(alias Abu Jihad). Date of birth:",
    # Chataev: missing sentence stop before "Date of birth:".
    "(d) Odnorukiy) Date of birth:": "(d) Odnorukiy). Date of birth:",
    # Gaziev: this entry separates its label sentences with commas.
    "(l) Abu-Naser), Date of birth: 11.11.1965, Place of birth: Itum-Kale": (
        "(l) Abu-Naser). Date of birth: 11.11.1965. Place of birth: Itum-Kale"
    ),
    "Itum-Kalinskiy District, Republic of Chechnya, Russian Federation, Address: a)": (
        "Itum-Kalinskiy District, Republic of Chechnya, Russian Federation. Address: a)"
    ),
    "as at August 2015), Nationality: Russian Federation, Other information:": (
        "as at August 2015). Nationality: Russian Federation. Other information:"
    ),
    # Khalimov: "Date of birth" printed without its colon.
    "Gulmurod Khalimov. Date of birth (a) 14.5.1975": (
        "Gulmurod Khalimov. Date of birth: (a) 14.5.1975"
    ),
    # Revival of Islamic Heritage Society: the alias group never closes and
    # "Address" is glued to alias (f).
    "Al-Furqan Welfare Foundation Address:": "Al-Furqan Welfare Foundation). Address:",
    # Al Moulathamoun: the address sentence is printed inside the alias
    # group and the closing parenthesis sits after it.
    "(b) The Veiled. Address: (a) Algeria; (b) Mali; (c) Niger).": (
        "(b) The Veiled). Address: (a) Algeria; (b) Mali; (c) Niger."
    ),
    # Al-Mansur: the Arabic-script name parentheticals never close.
    "(name in Arabic script: منصور ال محمد مصطفى سالم; (b)": (
        "(name in Arabic script: منصور ال محمد مصطفى سالم); (b)"
    ),
    "(name in Arabic script: محمد مصطفى سالم. Address:": (
        "(name in Arabic script: محمد مصطفى سالم). Address:"
    ),
}

# Values that legitimately contain a known "Label:" string and must not be
# split further, keyed by the exact full value.
RESIDUE_ALLOW: frozenset[str] = frozenset(
    {
        # Zidane: the printed alias list annotates two aliases with their
        # own birth data; each annotated alias is one value.
        "Sayf-Al Adl. Date of birth: 11.4.1963. Place of birth: Monufia "
        "Governate, Egypt. Nationality: Egyptian",
        "Muhamad Ibrahim Makkawi. Date of birth: (i) 11.4.1960, "
        "(ii) 11.4.1963. Place of birth: Egypt. Nationality: Egyptian",
    }
)

# Reviewed hand-mappings for whole labelled values whose parts belong in
# other columns or mix a dropped kin line with kept prose, keyed by the
# exact printed value.
VALUE_OVERRIDES: dict[str, tuple[tuple[str, str], ...]] = {
    # Al-Baghdadi: the national identification is a ration card reference.
    "Ration card number: 0134852": (("idNumber", "0134852"),),
    # Ahmad / Al-Mazidih: a dropped kin line followed by kept prose.
    (
        "Mother's name: Masouma Abd al-Rahman. Photo available for inclusion "
        "in the INTERPOL-UN Security Council Special Notice"
    ): (
        (
            "notes",
            "Photo available for inclusion in the INTERPOL-UN Security "
            "Council Special Notice",
        ),
    ),
    # Al-Baqi: item (b) is a dropped kin line followed by kept prose.
    (
        "Mother's name: Nadira Ayoub Asaad. Photo available for inclusion in "
        "the INTERPOL-UN Security Council Special Notice"
    ): (
        (
            "notes",
            "Photo available for inclusion in the INTERPOL-UN Security "
            "Council Special Notice",
        ),
    ),
    # Al-Rimi: a dropped kin line followed by kept prose.
    (
        "Mother’s name: Fatima Muthanna Yahya. Photo available for inclusion "
        "in the INTERPOL-UN Security Council Special Notice. Leader of "
        "Al-Qaida in the Arabian Peninsula since Jun. 2015, pledged loyalty "
        "to Aiman al-Zawahiri. As of February 2020, reportedly killed in a "
        "counterterrorism operation in Yemen"
    ): (
        (
            "notes",
            "Photo available for inclusion in the INTERPOL-UN Security "
            "Council Special Notice. Leader of Al-Qaida in the Arabian "
            "Peninsula since Jun. 2015, pledged loyalty to Aiman "
            "al-Zawahiri. As of February 2020, reportedly killed in a "
            "counterterrorism operation in Yemen",
        ),
    ),
    # Global Relief Foundation: item (b) is a US tax identifier.
    "U.S. Federal Employer Identification: 36-3804626": (("taxNumber", "36-3804626"),),
    # Al-Dari: a father's-name line followed by kept prose.
    (
        "Harith bin Salman Al-Dari bin Mahmud al-Shammari. Photo available "
        "for inclusion in the INTERPOL-UN Security Council Special Notice"
    ): (
        ("fatherName", "Harith bin Salman Al-Dari bin Mahmud al-Shammari"),
        (
            "notes",
            "Photo available for inclusion in the INTERPOL-UN Security "
            "Council Special Notice",
        ),
    ),
    # Kotey: the printed sub-item list runs across a segment boundary.
    "beard; (d) Ethnic background: Ghanaian Cypriot": (
        ("appearance", "beard"),
        ("ethnicity", "Ghanaian Cypriot"),
    ),
    # El Sheikh: as above; the kin sub-item is dropped.
    "beard; (d) Mother’s name: Maha Elgizouli": (("appearance", "beard"),),
    # Ismailov: the printed sub-item list runs across a segment boundary.
    (
        "long face, speech defect, b) Photo available for inclusion in the "
        "INTERPOL-UN Security Council Special Notice"
    ): (
        ("appearance", "long face, speech defect"),
        (
            "notes",
            "Photo available for inclusion in the INTERPOL-UN Security "
            "Council Special Notice",
        ),
    ),
}

# Printed startDate defects, exact printed text → the intended date.
DATE_PINS: dict[str, str] = {
    # Ummah Tameer E-Nau: a stray space inside the printed date.
    "24.12. 2001": "24.12.2001",
}

# Legacy entries that do not follow the entry grammar at all, hand
# transcribed and keyed by their exact printed text. Any source change
# breaks the key and forces re-review.
ENTRY_OVERRIDES: dict[str, tuple[tuple[str, str], ...]] = {
    # 1990s shorthand: "born 1947" is the printed birth label.
    "Bin Marwan, Bilal; born 1947.": (
        ("name", "Bin Marwan, Bilal"),
        ("birthDate", "1947"),
    ),
    # OFAC-era shorthand with "aka" aliases and unlabelled address.
    (
        "Mahmood, Sultan Bashir-Ud-Din (aka Mahmood, Sultan Bashiruddin; aka "
        "Mehmood, Dr. Bashir Uddin; aka Mekmud, Sultan Baishiruddin), Street "
        "13, Wazir Akbar Khan, Kabul, Aghanistan; alt. date of birth 1937; "
        "alt. date of birth 1938; alt. date of birth 1939; alt. date of birth "
        "1940; alt. date of birth 1941; alt. date of birth 1942; alt. date of "
        "birth 1943; alt. date of birth 1944; alt. date of birth 1945; "
        "nationality: Pakistani."
    ): (
        ("name", "Mahmood, Sultan Bashir-Ud-Din"),
        ("alias", "Mahmood, Sultan Bashiruddin"),
        ("alias", "Mehmood, Dr. Bashir Uddin"),
        ("alias", "Mekmud, Sultan Baishiruddin"),
        ("address", "Street 13, Wazir Akbar Khan, Kabul, Aghanistan"),
        ("birthDate", "1937"),
        ("birthDate", "1938"),
        ("birthDate", "1939"),
        ("birthDate", "1940"),
        ("birthDate", "1941"),
        ("birthDate", "1942"),
        ("birthDate", "1943"),
        ("birthDate", "1944"),
        ("birthDate", "1945"),
        ("nationality", "Pakistani"),
    ),
    # OFAC-era shorthand; the birth statement and prose stay verbatim.
    (
        "Muhammad 'Atif (aka Abu Hafs); born (probably) 1944, Egypt; thought "
        "to be an Egyptian national; senior lieutenant to Usama Bin Laden"
    ): (
        ("name", "Muhammad 'Atif"),
        ("alias", "Abu Hafs"),
        ("birthDate", "(probably) 1944"),
        ("birthPlace", "Egypt"),
        ("notes", "thought to be an Egyptian national"),
        ("notes", "senior lieutenant to Usama Bin Laden"),
    ),
}


def verbatim_date(text: str, ctx: str) -> str:
    # Dotted and UN-abbreviated forms occur in this document. The printed
    # wording is kept; the recognizers only guard the shape.
    if parse_dotted_date(text) is None and parse_abbrev_date(text) is None:
        raise ParseError(f"{ctx}: unrecognized date {text!r}")
    return text


def check_registry() -> None:
    program = get_program_by_key(PROGRAM_KEY)
    if program is None:
        raise ParseError(f"unknown program key {PROGRAM_KEY!r}")
    if MEASURE not in get_args(Measure):
        raise ParseError(f"invalid measure {MEASURE!r}")
    if MEASURE not in program.measures:
        raise ParseError(f"measure {MEASURE!r} not in {PROGRAM_KEY}")
    for _, schema_name in SECTIONS:
        if model.get(schema_name) is None:
            raise ParseError(f"unknown schema {schema_name!r}")


def enum_paren(text: str, i: int) -> bool:
    """True when the ")" at i belongs to a bare "x)" enumeration marker.

    UN-style lists enumerate with unbracketed letters ("a) Muhsin Fadhil …
    b) …"); their closing parentheses have no opener and must not count as
    group closers. A bracketed "(a)" marker still balances normally.
    """
    if i < 1 or text[i] != ")":
        return False
    if not text[i - 1].islower() or not text[i - 1].isalpha():
        return False
    before = text[i - 2] if i >= 2 else " "
    return before in (" ", ":")


def paren_step(text: str, i: int, depth: int) -> int:
    """Track parenthesis depth, ignoring enumeration-marker closers."""
    char = text[i]
    if char == "(":
        return depth + 1
    if char == ")" and not enum_paren(text, i):
        return depth - 1
    return depth


def repair_unclosed(text: str, ctx: str) -> str:
    """Close an alias group the printer left unclosed.

    A recurring defect: the closing parenthesis before the first "Label:"
    sentence is missing ("… (e) Egyptian Islamic Movement. Other
    information: …"). When exactly one group stays open through the end of
    the entry, it is closed before the first sentence-boundary label found
    inside it. Any other imbalance stays an error.
    """
    depth = 0
    for i in range(len(text)):
        depth = paren_step(text, i, depth)
        if depth < 0:
            raise ParseError(f"{ctx}: stray closing parenthesis")
    if depth == 0:
        return text
    if depth != 1:
        raise ParseError(f"{ctx}: {depth} unclosed parentheses")
    depth = 0
    for i in range(len(text)):
        depth = paren_step(text, i, depth)
        if depth == 1 and text[i - 2 : i] in (". ", "; "):
            if DESIG_AT_RE.match(text, i) or LABEL_AT_RE.match(text, i):
                return text[: i - 2] + ")" + text[i - 2 :]
    raise ParseError(f"{ctx}: unclosed parenthesis without a label to close at")


def find_boundaries(text: str, ctx: str) -> list[tuple[int, int, str]]:
    """Locate label starts at sentence boundaries outside parentheses.

    Returns (start, value_start, label) triples; the designation label is
    reported as the pseudo-label "".
    """
    bounds: list[tuple[int, int, str]] = []
    depth = 0
    i = 0
    while i < len(text):
        depth = paren_step(text, i, depth)
        if depth < 0:
            raise ParseError(f"{ctx}: unbalanced parentheses")
        if depth == 0:
            at_boundary = i == 0 or text[i - 2 : i] in (". ", "; ")
            if not at_boundary and text[i - 1 : i] == " ":
                for glued, offset in GLUED_LABEL_PINS.items():
                    if text.startswith(glued, i - offset):
                        at_boundary = True
                        break
            if at_boundary:
                desig = DESIG_AT_RE.match(text, i)
                if desig is not None:
                    bounds.append((i, desig.end(), ""))
                    i = desig.end()
                    continue
                label = LABEL_AT_RE.match(text, i)
                if label is not None:
                    matched = text[i : label.end()].rstrip()
                    bounds.append((i, label.end(), matched.rstrip(":")))
                    i = label.end()
                    continue
                dotted = DOTLBL_AT_RE.match(text, i)
                if dotted is not None:
                    matched = text[i : dotted.end()].rstrip()
                    bounds.append((i, dotted.end(), matched))
                    i = dotted.end()
                    continue
        i += 1
    if depth != 0:
        raise ParseError(f"{ctx}: unbalanced parentheses")
    return bounds


def check_residue(ctx: str, column: str, value: str) -> None:
    """A known label at depth 0 of a stored value means a missed split.

    Labels inside nested parentheses are the source's own sub-structure of
    one value (an alias annotated with its holder's birth data) and stay.
    """
    if value in RESIDUE_ALLOW:
        return
    depth = 0
    for i in range(len(value)):
        depth = paren_step(value, i, depth)
        if depth != 0:
            continue
        match = RESIDUE_RE.match(value, i) or DESIG_AT_RE.match(value, i)
        if match is not None and (i == 0 or not value[i - 1].isalnum()):
            raise ParseError(
                f"{ctx}: label {match.group(0)[:40]!r} left inside "
                f"{column} value {value[:60]!r}"
            )


def paren_groups(text: str, ctx: str) -> list[tuple[int, int]]:
    groups: list[tuple[int, int]] = []
    depth = 0
    start = 0
    for i in range(len(text)):
        if text[i] == "(" and depth == 0:
            start = i
        new_depth = paren_step(text, i, depth)
        if new_depth == 0 and depth == 1:
            groups.append((start, i + 1))
        if new_depth < 0:
            raise ParseError(f"{ctx}: unbalanced name {text[:60]!r}")
        depth = new_depth
    if depth != 0:
        raise ParseError(f"{ctx}: unbalanced name {text[:60]!r}")
    return groups


def split_enum(content: str, ctx: str) -> list[str]:
    """Split an alias list on its printed enumeration, if it has one.

    Observed forms: "(a) X, (b) Y", "a) X b) Y", and plain single values.
    Enumeration letters must run consecutively from "a"; splitting happens
    only at parenthesis depth 0. Commas inside one item stay put.
    """
    content = content.strip()
    if not content:
        raise ParseError(f"{ctx}: empty alias list")
    marks: list[tuple[int, int]] = []
    letter = "a"
    depth = 0
    i = 0
    while i < len(content):
        char = content[i]
        if char == "(":
            # "(a) " enumeration markers do not open a group.
            if depth == 0 and re.match(r"^\(" + letter + r"\) ", content[i : i + 4]):
                marks.append((i, i + 4))
                letter = chr(ord(letter) + 1)
                i += 4
                continue
        if depth == 0 and char == letter and content[i + 1 : i + 3] == ") ":
            boundary = i == 0 or content[i - 1] == " "
            if boundary:
                marks.append((i, i + 3))
                letter = chr(ord(letter) + 1)
                i += 3
                continue
        depth = paren_step(content, i, depth)
        if depth < 0:
            raise ParseError(f"{ctx}: unbalanced alias list {content[:60]!r}")
        i += 1
    if not marks:
        return [content.rstrip(",;")]
    if marks[0][0] != 0:
        raise ParseError(f"{ctx}: alias list not starting at (a): {content[:60]!r}")
    items: list[str] = []
    for idx, (_, value_start) in enumerate(marks):
        end = marks[idx + 1][0] if idx + 1 < len(marks) else len(content)
        item = content[value_start:end].strip().rstrip(",;").strip()
        if not item:
            raise ParseError(f"{ctx}: empty alias item in {content[:60]!r}")
        items.append(item)
    return items


def parse_alias_group(ctx: str, content: str, row: Row) -> bool:
    """Consume a parenthetical of sublabelled name lists; False if unlabeled."""
    match = GROUP_SUBLABEL_RE.match(content)
    if match is None:
        return False
    # Split the content at depth-0 sublabel occurrences (start or "; ").
    bounds: list[tuple[int, int, str]] = []
    depth = 0
    i = 0
    while i < len(content):
        depth = paren_step(content, i, depth)
        if depth == 0 and (i == 0 or content[i - 2 : i] == "; "):
            sub = GROUP_SUBLABEL_RE.match(content, i)
            if sub is not None:
                matched = content[i : sub.end()].rstrip().rstrip(":")
                bounds.append((i, sub.end(), matched.lower()))
                i = sub.end()
                continue
        i += 1
    for idx, (start, value_start, sublabel) in enumerate(bounds):
        end = bounds[idx + 1][0] if idx + 1 < len(bounds) else len(content)
        value = content[value_start:end].strip().rstrip(";").strip()
        if sublabel in SCRIPT_SUBLABELS and not value:
            # An embedded image the parser cannot transcribe; dropped.
            continue
        column = GROUP_SUBLABEL_COLUMNS[sublabel]
        if sublabel in SCRIPT_SUBLABELS:
            row.add(column, [value])
            continue
        for item in split_enum(value, ctx):
            check_residue(ctx, column, item)
            row.add(column, [item])
    return True


def set_start_date(ctx: str, row: Row, printed: str) -> None:
    if row.start_date:
        raise ParseError(f"{ctx}: second designation date")
    printed = DATE_PINS.get(printed, printed)
    row.start_date = verbatim_date(printed, ctx)


def route_item(ctx: str, item: str, row: Row) -> None:
    """Place one printed "Other information" item in its proper column.

    Sub-items carry their own labels ("(c) Established on …; (d) Website:
    …"); a labelled sub-item routes exactly like a sentence-level label,
    per the contract's rule that identifiers embedded in prose lines belong
    in their proper columns. Unlabeled items are bare notes values.
    """
    mapped = VALUE_OVERRIDES.get(item)
    if mapped is not None:
        for target, target_value in mapped:
            row.add(target, [target_value])
        return
    desig = DESIG_AT_RE.match(item)
    if desig is not None:
        printed = item[desig.end() :].strip().rstrip(".")
        set_start_date(ctx, row, printed)
        return
    label_match = LABEL_AT_RE.match(item)
    if label_match is not None:
        label = item[: label_match.end()].rstrip().rstrip(":")
        column = INFO_LABELS[label]
        value = item[label_match.end() :].strip()
        if not value:
            raise ParseError(f"{ctx}: empty item value for {label!r}")
        if column == "":
            # Kin names have no column; deliberate drop. A multi-sentence
            # value carries more than the kin line — pin it instead.
            if ". " in value:
                raise ParseError(f"{ctx}: prose beyond kin line {value[:60]!r}")
            return
        if column == "notes":
            raise ParseError(f"{ctx}: nested notes label {label!r}")
        check_residue(ctx, column, value)
        row.add(column, [value])
        return
    check_residue(ctx, "notes", item)
    row.add("notes", [item])


def handle_notes_value(ctx: str, value: str, row: Row) -> None:
    if value.startswith("(a) ") or value.startswith("a) "):
        items = split_enum(value, ctx)
    else:
        items = [value]
    for item in items:
        route_item(ctx, item, row)


def parse_name_region(ctx: str, text: str, row: Row) -> None:
    region = text.strip()
    if region.endswith(".") or region.endswith(";"):
        region = region[:-1].rstrip()
    groups = paren_groups(region, ctx)
    cursor = 0
    lead = region[: groups[0][0]].strip() if groups else region
    if not lead:
        raise ParseError(f"{ctx}: entry starts with a parenthetical: {region[:60]!r}")
    consumed_spans: list[tuple[int, int]] = []
    for start, end in groups:
        between = region[cursor:start].strip()
        if cursor > 0 and between not in ("", ",", ";"):
            raise ParseError(f"{ctx}: text between name groups: {between[:60]!r}")
        cursor = end
        content = region[start + 1 : end - 1].strip()
        if not content:
            # An embedded native-script image the parser cannot transcribe.
            consumed_spans.append((start, end))
            continue
        if parse_alias_group(ctx, content, row):
            consumed_spans.append((start, end))
            continue
        if lead in FUNCTION_PAREN_LEADS:
            row.add("position", [content])
            consumed_spans.append((start, end))
            continue
        # Unlabeled parentheticals stay in the name.
    tail = region[groups[-1][1] :].strip() if groups else ""
    if tail:
        raise ParseError(f"{ctx}: text after name groups: {tail[:60]!r}")
    # Rebuild the printed name minus the consumed groups.
    name = region
    for start, end in reversed(consumed_spans):
        name = name[:start] + name[end:]
    name = " ".join(name.split()).strip().rstrip(",;").strip()
    if not name:
        raise ParseError(f"{ctx}: empty name in {region[:60]!r}")
    row.add("name", [name])


def parse_entry(annex: str, schema: str, text: str, entry_no: int) -> Row:
    ctx = f"{annex} entry {entry_no}"
    row = Row(annex, schema, MEASURE)
    override = ENTRY_OVERRIDES.get(text)
    if override is not None:
        for column, value in override:
            if column == "startDate":
                row.start_date = verbatim_date(value, ctx)
            else:
                row.add(column, [value])
        return row
    for defect, repaired in MISPRINT_REPAIRS.items():
        if defect in text:
            text = text.replace(defect, repaired)
    text = repair_unclosed(text, ctx)
    bounds = find_boundaries(text, ctx)
    name_end = bounds[0][0] if bounds else len(text)
    parse_name_region(ctx, text[:name_end], row)
    for idx, (start, value_start, label) in enumerate(bounds):
        end = bounds[idx + 1][0] if idx + 1 < len(bounds) else len(text)
        value = text[value_start:end].strip()
        if value.endswith(".") or value.endswith(";"):
            value = value[:-1].rstrip()
        if not value:
            raise ParseError(f"{ctx}: empty value for {label!r}")
        if label == "":  # the designation date
            if idx + 1 != len(bounds):
                raise ParseError(f"{ctx}: designation date is not last")
            set_start_date(ctx, row, value)
            continue
        column = INFO_LABELS[label]
        if column in NAME_LIST_COLUMNS:
            for item in split_enum(value, ctx):
                check_residue(ctx, column, item)
                row.add(column, [item])
            continue
        if value in PLACEHOLDER_VALUES:
            continue
        mapped = VALUE_OVERRIDES.get(value)
        if mapped is not None:
            for target, target_value in mapped:
                row.add(target, [target_value])
            continue
        if column == "":
            continue  # kin names have no column; deliberate drop
        if column == "notes":
            handle_notes_value(ctx, value, row)
            continue
        check_residue(ctx, column, value)
        row.add(column, [value])
    return row


def entry_text(annex: str, element: Element, entry_no: int) -> str:
    ctx = f"{annex} entry {entry_no}"
    cls = element.get("class") or ""
    if "grid-container" in cls:
        children = [child for child in element if isinstance(child.tag, str)]
        if len(children) != 2:
            raise ParseError(f"{ctx}: grid item with {len(children)} children")
        dash, body = children
        if (dash.get("class") or "") != "list grid-list-column-1":
            raise ParseError(f"{ctx}: unexpected grid column {dash.get('class')!r}")
        if clean(element_text(dash), ctx) != "—":
            raise ParseError(f"{ctx}: unexpected grid bullet")
        if (body.get("class") or "") != "grid-list-column-2":
            raise ParseError(f"{ctx}: unexpected grid column {body.get('class')!r}")
        inner = xpath_elements(body, "./div[@class='list']", expect_exactly=1)[0]
        return bare_text(inner, ctx)
    return bare_text(element, ctx)


def parse_annex_i(annex: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    sections = list(SECTIONS)
    schema: str | None = None
    note_expected = False
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls in ("", "title-annex-1"):
            continue
        if child.tag == "p" and cls == "modref":
            check_marker(" ".join(element_text(child).split()), annex)
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-1":
            if clean(element_text(child), annex) != ANNEX_I_TITLE:
                raise ParseError(f"{annex}: unexpected annex title")
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-2":
            text = clean(element_text(child), annex)
            if note_expected and text == SECTION_NOTE:
                note_expected = False
                continue
            if not sections or text != sections[0][0]:
                raise ParseError(f"{annex}: unexpected section {text!r}")
            schema = sections.pop(0)[1]
            note_expected = schema == "Person"
            continue
        if child.tag == "div" and (cls == "list" or "grid-container" in cls):
            if schema is None:
                raise ParseError(f"{annex}: entry before any section heading")
            text = clean(entry_text(annex, child, len(rows) + 1), annex)
            rows.append(parse_entry(annex, schema, text, len(rows) + 1))
            continue
        raise ParseError(f"{annex}: unexpected <{child.tag} class={cls!r}>")
    if sections:
        raise ParseError(f"{annex}: missing section {sections[0][0]!r}")
    return rows


def parse_annex_ia(annex: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    seen_title = False
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls in ("", "title-annex-1"):
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-1":
            if clean(element_text(child), annex) != ANNEX_IA_TITLE:
                raise ParseError(f"{annex}: unexpected annex title")
            seen_title = True
            continue
        if child.tag == "p" and cls == "norm":
            text = clean(element_text(child), annex)
            rows.append(parse_entry(annex, "Person", text, len(rows) + 1))
            continue
        raise ParseError(f"{annex}: unexpected <{child.tag} class={cls!r}>")
    if not seen_title:
        raise ParseError(f"{annex}: missing annex title")
    return rows


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    for annex, block in annex_blocks(doc, {"I", "IA"} | NON_TARGET):
        if annex in NON_TARGET:
            continue
        annex_rows = (
            parse_annex_i(annex, block)
            if annex == "I"
            else parse_annex_ia(annex, block)
        )
        if not annex_rows:
            raise ParseError(f"{annex}: no entries extracted")
        rows.extend(annex_rows)
    return rows


@click.command(help="Parse consolidated Regulation 881/2002 into a CSV candidate.")
@click.argument("celex")
@click.option(
    "--source",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Parse these exact XHTML bytes instead of fetching from CELLAR.",
)
def main(celex: str, source: Path | None) -> None:
    try:
        check_registry()
        if CONSOLIDATED_RE.match(celex) is None:
            raise ParseError(f"not a consolidated 881/2002 CELEX: {celex!r}")
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
