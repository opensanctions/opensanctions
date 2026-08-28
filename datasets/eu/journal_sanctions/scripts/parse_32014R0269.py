"""Parse consolidated Regulation (EU) 269/2014 (Ukraine) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — the Article 2 fund-freeze list, the EU's largest: parts
  "Persons" and "Entities" (no part letters; numbering restarts per part,
  so the annex identifiers are I.PERSONS and I.ENTITIES), each one
  five-column table (entry number, name, identifying information, reasons,
  date of listing). Travel bans live in Decision 2014/145/CFSP.
- Annex II — competent-authority websites, not designations.

Name cells print the Latin name on the first line — kept whole, including
inline parentheticals and a.k.a. tails, per the contract's unlabeled-
combined-form rule — and further renderings one per line: parenthesized
native-script or annotated lines, bare Latin or Cyrillic transliteration
variants, and labelled a.k.a. lists. Every such further line is an alias
(split on ";" only); a parenthesized rendering may wrap across lines and
is joined until its closing parenthesis; "Formerly known as …" lines are
previousName. Entry numbers may carry a lowercase insertion suffix
("174a."). A handful of rows continue the previous entry (empty number
cell, or the two pinned four-cell rows missing the number column, or one
pinned single-cell reasons row).

Identifying-information cells are label-driven with a large vocabulary.
Entity entries printing a KPP-family or IMO-family label are emitted as
Company (kppCode and imoNumber exist only there). Labels with no contract
column are deliberately dropped and documented: relational lines naming
other parties (Associated …, Parent, Owner, Liquidator, General Director,
CEO, Subsidiaries), identity-document attributes (Date of issue/expiry,
Issued by), Russian statistical codes (OKFS, OKOGU, OKOPF, FSFR, OKATO),
and identifier systems the contract cannot hold (blockchain wallets and
commercial data-vendor IDs). Dates are transcribed as the source prints
them ("21.7.2014"); the crawler normalizes dates.

Output: data/consolidated/32014R0269.csv (the EU Journal consolidated CSV
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
    cell_lines,
    check_consolidated_celex,
    check_marker,
    check_registry,
    clean,
    load_source,
    split_values,
    summary,
    to_record,
    validate_records,
    verbatim_date,
    write_csv,
)
from lxml import html
from zavod.helpers.html import element_text, xpath_elements
from zavod.util import Element

FRAMEWORK_CELEX = "32014R0269"
PROGRAM_KEY = "EU-UKR"
# Annex I implements the regulation's Article 2 fund freeze; travel bans
# live in Decision 2014/145/CFSP.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"II"})
SUBTITLE = (
    "List of natural and legal persons, entities and bodies referred to in Article 2"
)
# (part heading, part id, default schema) in print order; numbering
# restarts per part, so the part name joins the annex identifier.
PARTS = (
    ("Persons", "PERSONS", "Person"),
    ("Entities", "ENTITIES", "LegalEntity"),
)
HEADER = ("", "Name", "Identifying information", "Reasons", "Date of listing")

# Entry numbers, including corrigendum-inserted lowercase suffixes ("174a.").
NUMBER_RE = re.compile(r"^(\d+)([a-z]?)\.$")
# a.k.a./alias label prefixes as printed: "a.k.a.", "a.k.a", "A.k.a.",
# "AKA", "Alias:", "alias ", optionally glued to the value ("a.k.a.ECOOIL").
AKA_RE = re.compile(r"^(?:a\.k\.a\.?:? ?|A\.k\.a\.:? ?|AKA:? ?|[Aa]lias:? )(.+)$")
BARE_AKA_RE = re.compile(r"^(?:a\.k\.a\.?|A\.k\.a\.|AKA|[Aa]lias):?$")
# Leading and mid-value a.k.a. labels inside one printed rendering: the
# label is structure, never part of the alias value, and a repeated label
# marks a further alias (Myanmar precedent).
AKA_STRIP_RE = re.compile(r"^(?:a\.k\.a\.?|A\.k\.a\.|AKA|[Aa]lias):? ?")
AKA_SPLIT_RE = re.compile(r" a\.k\.a\.?:? ")
# Bare language-annotation lines label the following or same-line rendering
# ("Russian: Общество …", "Arabic:").
LANG_LINE_RE = re.compile(r"^(?:Russian|Ukrainian|Belarusian|Arabic): ?(.*)$")
# "Formerly known as …" renderings, parenthesized or bare → previousName.
FKA_RE = re.compile(r"^\(?[Ff]ormerly known as[:,]? (.+?)\)?$")
# Fully parenthesized rendering; groups may wrap across lines until the
# closing parenthesis (a trailing list comma/semicolon after it is print
# punctuation and stripped).
PAREN_LINE_RE = re.compile(r"^\((.+)\)[,;]?$")
GROUP_CLOSE_RE = re.compile(r"^(.*)\)[,;]?$")
# Language-annotation prefixes inside parenthesized renderings are printed
# labels ("Russian: …"), stripped so the alias holds the rendering only.
LANG_PREFIX_RE = re.compile(
    r"^(?:Russian|Ukrainian|Belarusian|Uzbek|Serbian|Croatian)(?: spelling)?: ?(.+)$"
)

# Identifying-information labels → CSV column, exactly as printed
# (whitespace-stripped). Assembled from the profiled vocabulary of the
# 20260717 consolidated text; an unknown label is a review event.
INFO_LABELS = {
    # birth
    "DOB": "birthDate",
    "D.O.B": "birthDate",
    "Date of birth": "birthDate",
    "Possible DOB": "birthDate",
    "POB": "birthPlace",
    "Place of birth": "birthPlace",
    # person basics
    "Gender": "gender",
    "Nationality": "nationality",
    "Nationalities": "nationality",
    # roles
    "Function": "position",
    "Functions": "position",
    "Position": "position",
    "Position(s)": "position",
    "Rank": "position",
    "Military rank": "position",
    "Profession": "position",
    # passports
    "Passport": "passportNumber",
    "Passport number": "passportNumber",
    "Passport numbers": "passportNumber",
    "Passport No": "passportNumber",
    "International passport number": "passportNumber",
    "Israeli passport number": "passportNumber",
    "Israel passport number": "passportNumber",
    "Russian passport numbers": "passportNumber",
    # generic identity documents
    "ID number": "idNumber",
    "ID Number": "idNumber",
    "National ID": "idNumber",
    "National ID number": "idNumber",
    "National ID no": "idNumber",
    "Personal ID": "idNumber",
    "Identity document": "idNumber",
    "Identity document number": "idNumber",
    "Passport or ID number": "idNumber",
    "Passport/ID numbers": "idNumber",
    "Entity-based ID": "idNumber",
    "Unique Entity Identifier (SAM)": "idNumber",
    # tax identifiers (INN-explicit forms → innCode)
    "INN": "innCode",
    "INN/TIN": "innCode",
    "Tax ID (INN no.)": "innCode",
    "Tax Identification Number (ИНН)": "innCode",
    "Taxpayer Identification number (INN)": "innCode",
    "Russian taxpayer number (INN)": "innCode",
    "TIN": "taxNumber",
    "Tax number": "taxNumber",
    "Tax Number": "taxNumber",
    "Tax ID": "taxNumber",
    "Tax ID No.": "taxNumber",
    "Tax ID No": "taxNumber",
    "Tax ID number": "taxNumber",
    "Tax payer ID": "taxNumber",
    "Tax Identification Number": "taxNumber",
    "Tax Identification Number (Ukraine)": "taxNumber",
    "Tax Identification number": "taxNumber",
    "Tax identification Number": "taxNumber",
    "Tax identification number": "taxNumber",
    "Tax Identificiation Number": "taxNumber",
    "Tax Indentification Number": "taxNumber",
    "Taxpayer Identification Number": "taxNumber",
    "Taxpayer identification number": "taxNumber",
    "Taxpayer Identification number": "taxNumber",
    "Individual tax number": "taxNumber",
    "Individual Tax Number": "taxNumber",
    "Tax Individual Number": "taxNumber",
    "Tax individual number": "taxNumber",
    "National Tax ID": "taxNumber",
    "Company Tax Identification Number": "taxNumber",
    "Tax registration number": "taxNumber",
    "Tax Registration Number": "taxNumber",
    "VAT": "taxNumber",
    "VAT ID": "taxNumber",
    "VAT number": "taxNumber",
    "UNP": "taxNumber",
    # Russian company codes
    "KPP": "kppCode",
    "— KPP": "kppCode",
    "PPC": "kppCode",
    "Registration reason code": "kppCode",
    "OGRN": "ogrnCode",
    "— OGRN": "ogrnCode",
    "OGRN number": "ogrnCode",
    "PSRN": "ogrnCode",
    "Primary State registration number": "ogrnCode",
    "Primary state registration number (OGRN)": "ogrnCode",
    "Main state registration number": "ogrnCode",
    "Registration ID (OGRN no.)": "ogrnCode",
    "Registration number (OGRN)": "ogrnCode",
    "ОГРН/main state registration number": "ogrnCode",
    "OKPO": "okpoCode",
    "— OKPO": "okpoCode",
    # registration numbers
    "Reg. number": "registrationNumber",
    "Registration number": "registrationNumber",
    "Registration Number": "registrationNumber",
    "Registration No.": "registrationNumber",
    "Reigstration number": "registrationNumber",
    "Registration numbers": "registrationNumber",
    "Registration ID": "registrationNumber",
    "State registration number": "registrationNumber",
    "State Registration Number": "registrationNumber",
    "Business registration number": "registrationNumber",
    "Buisness registration number": "registrationNumber",
    "Company registration number": "registrationNumber",
    "Trade register number": "registrationNumber",
    "Economic register number": "registrationNumber",
    "Economic Register Number (CBLS)": "registrationNumber",
    "CBLS": "registrationNumber",
    "Local licence number": "registrationNumber",
    "Licence number": "registrationNumber",
    "Licence Number": "registrationNumber",
    "Business Licence No.": "registrationNumber",
    "Import and export enterprise code": "registrationNumber",
    "China Company Registration Number": "registrationNumber",
    "Native Belize company number": "registrationNumber",
    "Corporate Identification Number (CIN)": "registrationNumber",
    "Corportate Identification Number": "registrationNumber",
    "Unified social credit code": "registrationNumber",
    "Unified Social Credit Code": "registrationNumber",
    "China Unified Social Credit Code": "registrationNumber",
    "BIN/OGRN": "registrationNumber",
    # maritime, banking and market identifiers
    "IMO": "imoNumber",
    "IMO number": "imoNumber",
    "IMO-number": "imoNumber",
    "IMO registration number": "imoNumber",
    "IMO Number": "imoNumber",
    "SWIFT/BIC": "swiftBic",
    "LEI": "leiCode",
    "Legal Entity Identifier": "leiCode",
    "Stock code (Shenzhen Stock Exchange)": "ticker",
    # incorporation
    "Date of registration": "incorporationDate",
    "Date of Registration": "incorporationDate",
    "Registration date": "incorporationDate",
    "Registration Date": "incorporationDate",
    "Date of initial registration": "incorporationDate",
    "Date of creation": "incorporationDate",
    "Date of incorporation": "incorporationDate",
    "Founded": "incorporationDate",
    # entity form and name
    "Type of entity": "legalForm",
    "Type of entitiy": "legalForm",
    "Type of company": "legalForm",
    "Full corporate name": "alias",
    # addresses
    "Address": "address",
    "Addresses": "address",
    "Adress": "address",
    "Address (Work)": "address",
    "Address (HQ)": "address",
    "Address of Headquarters": "address",
    "Headquarters": "address",
    "Legal address": "address",
    "Legal Address": "address",
    "Former address": "address",
    "Former legal address": "address",
    "Postal address": "address",
    "Postal Address": "address",
    "Correspondence address": "address",
    "Contact address": "address",
    "Mailing address": "address",
    "Second address": "address",
    "Other addresses": "address",
    "Temporary address": "address",
    "Moscow office": "address",
    "Offices": "address",
    "Location": "address",
    "Location of activities": "address",
    "Suspected location": "address",
    "Place of registration": "address",
    "Place of Registration": "address",
    "Place of registation": "address",
    "Principal place of business": "address",
    "Principal places of business": "address",
    # contact
    "Telephone": "phone",
    # Printed with a Cyrillic "Т".
    "Тel": "phone",
    "Telephone.": "phone",
    "Telephones": "phone",
    "Tel.": "phone",
    "Tel": "phone",
    "Tel./fax": "phone",
    "Tel./Fax": "phone",
    "Phone": "phone",
    "Phone number": "phone",
    "Phone and fax": "phone",
    "Fax": "phone",
    "Telephone volunteers in Russia": "phone",
    "Email": "email",
    "EMail": "email",
    "email": "email",
    "e-mail": "email",
    "E-mail": "email",
    "Еmail": "email",
    "Mail": "email",
    "E-mail address": "email",
    "e-mail address": "email",
    "Website": "website",
    "Websites": "website",
    "Web": "website",
    "Webpage": "website",
    "Telegram": "website",
    "Telegram channel": "website",
    "Social media": "website",
    "Social network profile": "website",
    "Media resources": "website",
}
# Free-text labels whose prose value goes to `notes`, label stripped. Their
# values frequently smuggle labelled identifiers; each value is re-routed
# through the line rules first and only prose reaches notes.
NOTES_LABELS = frozenset(
    {
        "Other information",
        "Other identifying information",
        "Additional information",
        "Official information",
        "Social media and other information",
    }
)
# Labels with no CSV column, deliberately not transcribed. Relational lines
# name other parties; Date of issue/expiry and Issued by qualify the
# identity document printed above them; the dash-prefixed Russian
# statistical codes, blockchain wallets and commercial data-vendor
# identifiers have no contract columns. The label line and its bare
# continuation lines are consumed.
DROP_LABELS = frozenset(
    {
        "Associated company",
        "Associated entities",
        "Associated entites",
        "Associated entity",
        "Associated individuals",
        "Associated individual",
        "Associated individuals or entities",
        "Associated entities and individuals",
        "Associated persons",
        "Associated person",
        "Associates",
        "Other associated entities",
        "Parent",
        "Parent company",
        "Owner",
        "Liquidator",
        "General Director",
        "CEO",
        "Subsidiaries",
        "Date of issue",
        "Date of expiry",
        "Expiry Date",
        "— OKFS",
        "— OKOGU",
        "— OKOPF",
        "— FSFR",
        "— OKATO",
        "Known blockchain wallet addresses",
        "ETH",
        "BTC",
        "BSC",
        "TRX",
        "Acuris Unique ID",
        "Cedar Rose Internal Identifier",
        "EDI Global Issuer ID",
    }
)
# Labels the LABELLED_RE 40-char cap cannot match, checked as prefixes.
LONG_LABELS = {
    "Passport number, national ID number, other numbers of identity documents: ": "idNumber",
    "Address of the reception office in the Russian Federation: ": "address",
    "Telephone and fax numbers of the reception office in the Russian Federation: ": "phone",
    "Primary state identification number (OGRN): ": "ogrnCode",
    (
        "Date of registration in the Russian Federation (following the "
        "illegal referenda held in the occupied territories of Ukraine): "
    ): "incorporationDate",
}
# Colon-less labelled prefixes printed on bare lines.
COLONLESS_PREFIXES = (
    ("Tax ID No. ", "taxNumber"),
    ("Tax Identification Number ", "taxNumber"),
    ("Tax ID (INN no.) ", "innCode"),
    ("TIN/INN ", "innCode"),
    ("POB ", "birthPlace"),
    ("Nationality ", "nationality"),
    ("Gender ", "gender"),
    ("KPP ", "kppCode"),
    ("КПП ", "kppCode"),
    ("INN ", "innCode"),
    ("OGRN ", "ogrnCode"),
    ("PSRN ", "ogrnCode"),
    ("OKPO ", "okpoCode"),
    ("PPC ", "kppCode"),
    ("IEC ", "registrationNumber"),
    ("BIN ", "registrationNumber"),
    ("Unified Social Credit Code ", "registrationNumber"),
    ("Registration number ", "registrationNumber"),
    ("Tel. ", "phone"),
    ("Address ", "address"),
    ("Place of Registration ", "address"),
    ("Founded in ", "incorporationDate"),
    ("Founded on ", "incorporationDate"),
)
# Colon-less document-attribute lines, dropped like their labelled kin.
COLONLESS_DROPS = ("Issued by ",)
# Columns whose labelled value legitimately continues onto bare follow-on
# lines in this document (roles spanning paragraphs, address lists, notes
# prose, an entity-form qualifier). Bare lines after any other label are
# new structure.
CONTINUABLE_COLUMNS = frozenset(
    {"position", "address", "notes", "phone", "website", "legalForm"}
)

# Entity entries whose printed identifiers land in Company-only columns
# (kppCode from the KPP/PPC/registration-reason-code labels, imoNumber,
# a stock ticker) are emitted as Company; the printed structure forces
# the schema.
COMPANY_PROPS = ("kppCode", "imoNumber", "ticker")

# Entries whose parenthesized rendering is printed with its closing
# parenthesis lost (a recurring amending-act misprint, 881 precedent): the
# group still open at the cell end is closed under review.
UNCLOSED_GROUP_PINS = frozenset(
    {
        ("PERSONS", "394"),
        ("PERSONS", "1475"),
        ("PERSONS", "1725"),
        ("ENTITIES", "18"),
        ("ENTITIES", "135"),
        ("ENTITIES", "202"),
        ("ENTITIES", "300"),
    }
)
# Two entries print a language-labelled rendering wrapped mid-phrase onto
# the next line; the fragment is joined to its rendering.
NATIVE_WRAP_PINS = frozenset({("ENTITIES", "560"), ("ENTITIES", "597")})
# One entry ends its name cell with an a.k.a. label whose value was never
# printed (misprint); the dangling label is dropped under review.
DANGLING_AKA_PINS = frozenset({("ENTITIES", "691")})
# Reviewed hand-mappings for name-cell lines the rendering rules cannot
# place, keyed by (part, entry) and the exact line.
NAME_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    # A rendering group nesting an a.k.a. annotation with an internal ";"
    # (the ";" separates the two aliases inside the nested annotation).
    ("PERSONS", "140"): {
        "(Сергей Юрьевич КУЗОВЛЕВ": (("alias", "Сергей Юрьевич КУЗОВЛЕВ"),),
        "(a.k.a. Сергей; ИГНАТОВ, ТAMБOB))": (
            ("alias", "Сергей"),
            ("alias", "ИГНАТОВ, ТAMБOB"),
        ),
    },
    # The rendering's parenthesis closes mid-line and the listing extends
    # to the enterprise's branches (Belarus B25 precedent: inclusion lines
    # are notes; the branch names follow one per line).
    ("ENTITIES", "41"): {
        (
            "(‘Государственное унитарное предприятие Республики Крым "
            "‘Крымские морские порты’’), including branches:"
        ): (
            (
                "alias",
                "‘Государственное унитарное предприятие Республики Крым "
                "‘Крымские морские порты’’",
            ),
            ("notes", "including branches:"),
        ),
        "Feodosia Commercial Port,": (("notes", "Feodosia Commercial Port"),),
        "Kerch Ferry,": (("notes", "Kerch Ferry"),),
        "Kerch Commercial Port": (("notes", "Kerch Commercial Port"),),
    },
}

# Reviewed hand-mappings for identifying-information lines the rules cannot
# place, keyed by (part, entry) and the exact line. An empty mapping drops
# the line deliberately. Populated by the fail-closed iteration.
INFO_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    # A second, possibly-alternative birth place printed on a bare line.
    ("PERSONS", "132"): {
        (
            "or possibly Hirnytskyi village, Perevalsk district, Luhansk "
            "oblast, USSR (now Ukraine)"
        ): (
            (
                "birthPlace",
                "or possibly Hirnytskyi village, Perevalsk district, "
                "Luhansk oblast, USSR (now Ukraine)",
            ),
        ),
    },
    # The birth place's native rendering on a bare line.
    ("PERSONS", "142"): {
        "Невинномысск, Ставропольский край, Российская Федерация": (
            (
                "birthPlace",
                "Невинномысск, Ставропольский край, Российская Федерация",
            ),
        ),
    },
    # The DOB's year qualifier printed on its own line.
    ("PERSONS", "1017"): {
        "(year unknown)": (("birthDate", "(year unknown)"),),
    },
    # An unlabelled identifier line mixing a national ID with an
    # alternative passport (contiguous spans extracted under review).
    ("PERSONS", "1409"): {
        "654034325 (Russia); alt. Passport XXIIAH534753": (
            ("idNumber", "654034325 (Russia)"),
            ("passportNumber", "XXIIAH534753"),
        ),
    },
    # An award inside the other-identifying-information value; the award
    # label has no column and the prose reads bare.
    ("PERSONS", "1509"): {
        (
            "Other identifying information: Awards: Certificate of Merit "
            "of the President of the Russian Federation"
        ): (
            (
                "notes",
                "Certificate of Merit of the President of the Russian Federation",
            ),
        ),
    },
    # The birth place printed on a bare line without its label.
    ("PERSONS", "1766"): {
        "Novomoskovsk, Tula Oblast, USSR (now Russian Federation)": (
            (
                "birthPlace",
                "Novomoskovsk, Tula Oblast, USSR (now Russian Federation)",
            ),
        ),
    },
    # The info cell opens with the transliterated corporate name and an
    # a.k.a. line (name renderings printed in the wrong column).
    ("ENTITIES", "235"): {
        (
            "Obshchestvo S Ogranichennoi Otvetstvennostiu "
            "Nauchno-Tekhnicheskii Tsentr ‘Poisk-IT’"
        ): (
            (
                "alias",
                "Obshchestvo S Ogranichennoi Otvetstvennostiu "
                "Nauchno-Tekhnicheskii Tsentr ‘Poisk-IT’",
            ),
        ),
    },
    # A misprinted, truncated tax label ("TIN/:").
    ("ENTITIES", "346"): {
        "TIN/: 7718016666": (("taxNumber", "7718016666"),),
    },
    # A second registration event continuing the ";"-terminated value.
    ("ENTITIES", "477"): {
        "10.7.2023 (registered as a small enterprise)": (
            ("incorporationDate", "10.7.2023 (registered as a small enterprise)"),
        ),
    },
    # A dash-separated tax identifier line.
    ("ENTITIES", "697"): {
        "Russian Tax Identification Number - 0268061215": (
            ("taxNumber", "0268061215"),
        ),
    },
    # Unlabelled proceedings-status prose about the entity itself.
    ("ENTITIES", "38"): {
        "Ongoing bankruptcy proceedings.": (
            ("notes", "Ongoing bankruptcy proceedings."),
        ),
    },
    ("ENTITIES", "40"): {
        "In process of liquidation": (("notes", "In process of liquidation"),),
    },
}
# Entries whose date cell prints a stray trailing period or semicolon (list
# punctuation, not date wording), or nothing at all.
DATE_PERIOD_PINS = frozenset(
    {("PERSONS", "706"), ("PERSONS", "710"), ("PERSONS", "1819"), ("PERSONS", "1917")}
)
DATE_EMPTY_PINS = frozenset({("ENTITIES", "103")})
# Entry numbers printed without their trailing period, keyed by the exact
# printed number line.
NUMBER_PERIOD_PINS = frozenset({("PERSONS", "2097")})
# One reasons paragraph is printed as its own single-cell row.
SINGLE_CELL_REASON_PINS = frozenset({("PERSONS", "885")})
# Two rows print a continuation with one column missing entirely: entry
# 18's drops the date column (four cells = number/name/info/reasons),
# entry 434's drops the number column (four cells = name/info/reasons/date).
FOUR_CELL_CONTINUATION_PINS = {
    ("ENTITIES", "18"): "date-missing",
    ("ENTITIES", "434"): "number-missing",
}


# Only the dotted form occurs in this document.
DATE_FORMATS = ("dotted",)


def enclosing_parens(text: str) -> bool:
    """True when the first "(" pairs with the final ")" around the whole."""
    if not (text.startswith("(") and text.endswith(")")):
        return False
    depth = 0
    for index, char in enumerate(text):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index == len(text) - 1
    return False


def add_rendering(ctx: str, row: Row, text: str) -> None:
    """File one printed rendering: previousName by label, else alias.
    Trailing list punctuation, enclosing parentheses, a.k.a. labels and
    language annotations are printed structure, never part of the value;
    a lone unbalanced trailing parenthesis is a print artifact and shed."""
    text = text.strip().rstrip(",;").strip()
    if enclosing_parens(text):
        text = text[1:-1].strip()
    fka = FKA_RE.match(text)
    if fka is not None:
        row.add("previousName", [fka.group(1)])
        return
    for value in split_values(text):
        for piece in AKA_SPLIT_RE.split(value):
            piece = piece.strip().rstrip(",;").strip()
            while True:
                stripped = AKA_STRIP_RE.sub("", piece)
                if stripped == piece:
                    break
                piece = stripped.strip()
            lang = LANG_PREFIX_RE.match(piece)
            if lang is not None:
                piece = lang.group(1)
            if enclosing_parens(piece):
                piece = piece[1:-1].strip()
            if piece.endswith(")") and "(" not in piece:
                piece = piece[:-1].strip()
            if piece:
                row.add("alias", [piece])


def parse_name_extras(
    ctx: str, part: str, record_id: str, row: Row, lines: list[str]
) -> None:
    """Renderings printed under the name line: every line is an alias
    (or previousName by printed label); parenthesized groups may wrap."""
    overrides = NAME_OVERRIDES.get((part, record_id), {})
    group: list[str] | None = None
    pending_aka = False
    pending_lang = False
    for line in lines:
        if line in overrides:
            for column, value in overrides[line]:
                row.add(column, [value])
            group, pending_aka, pending_lang = None, False, False
            continue
        if group is not None:
            close = GROUP_CLOSE_RE.match(line)
            if close is not None:
                group.append(close.group(1))
                add_rendering(ctx, row, " ".join(group))
                group = None
            else:
                group.append(line)
            continue
        if pending_aka or pending_lang:
            pending_aka = pending_lang = False
            add_rendering(ctx, row, line)
            continue
        if BARE_AKA_RE.match(line):
            pending_aka = True
            continue
        aka = AKA_RE.match(line)
        if aka is not None:
            add_rendering(ctx, row, aka.group(1))
            continue
        lang_line = LANG_LINE_RE.match(line)
        if lang_line is not None:
            value = lang_line.group(1)
            if value == "":
                pending_lang = True
            elif (part, record_id) in NATIVE_WRAP_PINS:
                group = [value]
                continue
            else:
                add_rendering(ctx, row, value)
            continue
        paren = PAREN_LINE_RE.match(line)
        if paren is not None:
            add_rendering(ctx, row, paren.group(1))
            continue
        if line.startswith("("):
            group = [line[1:]]
            continue
        if line[0].islower():
            if (part, record_id) in NATIVE_WRAP_PINS:
                raise ParseError(f"{ctx}: wrap pin without open rendering")
            raise ParseError(f"{ctx}: suspected wrapped name line {line[:60]!r}")
        add_rendering(ctx, row, line)
    if group is not None:
        if (part, record_id) in NATIVE_WRAP_PINS:
            add_rendering(ctx, row, " ".join(group))
        elif (part, record_id) in UNCLOSED_GROUP_PINS:
            add_rendering(ctx, row, " ".join(group))
        else:
            raise ParseError(f"{ctx}: unterminated parenthesized rendering")
    if pending_aka:
        if (part, record_id) not in DANGLING_AKA_PINS:
            raise ParseError(f"{ctx}: dangling bare a.k.a. label")
    if pending_lang:
        raise ParseError(f"{ctx}: dangling language annotation")


def parse_name(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    # The first line is the name, kept whole: inline parentheticals and
    # a.k.a. tails are part of the printed combined form (the crawler's
    # name review categorises them).
    row.add("name", [lines[0]])
    parse_name_extras(ctx, part, record_id, row, lines[1:])


URL_RE = re.compile(r"^(?:https?:|www\.|t\.me/)\S+$")
PHONE_RE = re.compile(r"^\(?\+[\d\s()./,+-]+$")
EMAIL_RE = re.compile(r"^\S+@\S+$")


def classify_bare(line: str) -> tuple[str, str] | None:
    """Shape-classify a bare info line the label rules cannot place."""
    for long_label, column in LONG_LABELS.items():
        if line.startswith(long_label):
            return (column, line[len(long_label) :])
    if URL_RE.match(line):
        return ("website", line)
    if PHONE_RE.match(line):
        return ("phone", line)
    if EMAIL_RE.match(line.rstrip(",;")):
        return ("email", line.rstrip(",;"))
    for prefix, column in COLONLESS_PREFIXES:
        if line.startswith(prefix):
            return (column, line[len(prefix) :])
    return None


def parse_info(
    ctx: str, part: str, record_id: str, td: Element, row: Row
) -> tuple[set[str], bool]:
    """Parse the identifying-information cell; returns (labels seen,
    whether any line remained unplaced — always an error upstream)."""
    lines = cell_lines(td, ctx)
    overrides = INFO_OVERRIDES.get((part, record_id), {})
    seen: set[str] = set()
    block: str | None = None
    dropped = False
    seen_label = False
    opened_empty = False
    wrapped: str | None = None
    for line in lines:
        if line in overrides:
            for column, value in overrides[line]:
                row.add(column, [value])
            block, dropped, opened_empty, wrapped = None, False, False, None
            continue
        if PAREN_LINE_RE.match(line) and block == "address":
            # A parenthesized address detail continues the address block.
            row.add("address", [line])
            continue
        labelled = LABELLED_RE.match(line)
        label = labelled.group(1).strip() if labelled is not None else None
        if label in ("https", "http"):
            label, labelled = None, None
        if label in DROP_LABELS:
            block, dropped, seen_label = None, True, True
            opened_empty, wrapped = False, None
            seen.add(label or "")
            continue
        if label in NOTES_LABELS:
            assert labelled is not None
            seen.add(label or "")
            value = labelled.group(2)
            block, dropped, seen_label = "notes", False, True
            opened_empty, wrapped = False, None
            if value == "":
                continue
            routed = classify_bare(value) if LABELLED_RE.match(value) is None else None
            inner = LABELLED_RE.match(value)
            inner_label = inner.group(1).strip() if inner is not None else None
            if (
                inner is not None
                and inner_label is not None
                and inner_label in INFO_LABELS
            ):
                row.add(INFO_LABELS[inner_label], split_values(inner.group(2)))
                block = INFO_LABELS[inner_label]
            elif routed is not None:
                row.add(routed[0], [routed[1]])
            elif inner_label is not None and inner_label in DROP_LABELS:
                block, dropped = None, True
            elif inner_label is not None:
                raise ParseError(
                    f"{ctx}: unreviewed label inside notes value {line[:70]!r}"
                )
            else:
                row.add("notes", [value])
            continue
        if label is not None and label in INFO_LABELS:
            assert labelled is not None
            seen.add(label)
            column = INFO_LABELS[label]
            value = labelled.group(2)
            if value != "":
                row.add(column, split_values(value))
            block, dropped, seen_label = column, False, True
            # An empty-valued label holds its value on the following bare
            # line(s); a value ending in "," wraps mid-phrase onto the next
            # line (both 2020R1998 precedents).
            opened_empty = value == "" or value.endswith(";")
            wrapped = column if value.endswith(",") else None
            continue
        aka = AKA_RE.match(line)
        if aka is not None:
            # Name renderings occasionally print inside the info cell; the
            # printed a.k.a. label marks them.
            add_rendering(ctx, row, aka.group(1))
            block, dropped, opened_empty, wrapped = None, False, False, None
            continue
        shaped = classify_bare(line)
        if shaped is not None:
            row.add(shaped[0], [shaped[1]])
            block, dropped, opened_empty, wrapped = shaped[0], False, False, None
            continue
        if any(line.startswith(prefix) for prefix in COLONLESS_DROPS):
            block, dropped, opened_empty, wrapped = None, True, False, None
            continue
        if dropped:
            continue
        if wrapped is not None:
            row.props[wrapped][-1] = f"{row.props[wrapped][-1]} {line}"
            wrapped = None
            continue
        if not seen_label:
            # Unlabeled leading lines: role prose for persons, the address
            # for entities (both shapes shown by the document).
            row.add("position" if part == "PERSONS" else "address", [line])
            continue
        if block is not None and (opened_empty or block in CONTINUABLE_COLUMNS):
            row.add(block, split_values(line))
            continue
        raise ParseError(f"{ctx}: unrecognized info line {line[:70]!r}")
    return seen, False


def entry_schema(part: str, default: str, row: Row) -> str:
    if part == "ENTITIES" and any(prop in row.props for prop in COMPANY_PROPS):
        return "Company"
    return default


def parse_date_cell(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        if (part, record_id) in DATE_EMPTY_PINS:
            return
        raise ParseError(f"{ctx}: empty date cell")
    if len(lines) != 1:
        raise ParseError(f"{ctx}: {len(lines)} lines in date cell")
    value = lines[0]
    if (part, record_id) in DATE_PERIOD_PINS:
        value = value.rstrip(".;")
    row.start_date = verbatim_date(value, ctx, DATE_FORMATS)


def continue_row(ctx: str, part: str, row: Row, cells: list[Element]) -> None:
    """Merge a continuation row (empty number cell) into the previous
    entry: further renderings, identifying information, reasons prose."""
    ctx = f"{ctx} entry {row.record_id} (cont.)"
    name_lines = cell_lines(cells[0], ctx)
    if name_lines:
        parse_name_extras(ctx, part, row.record_id, row, name_lines)
    info_lines = cell_lines(cells[1], ctx)
    if info_lines:
        parse_info(ctx, part, row.record_id, cells[1], row)
    reason_lines = cell_lines(cells[2], ctx)
    if reason_lines:
        row.reason = (
            " ".join([row.reason, *reason_lines])
            if row.reason
            else " ".join(reason_lines)
        )
    if len(cells) > 3:
        date_lines = cell_lines(cells[3], ctx)
        if date_lines:
            raise ParseError(f"{ctx}: unexpected date content in continuation row")


def sort_key(record_id: str) -> tuple[int, str]:
    match = NUMBER_RE.match(f"{record_id}.")
    assert match is not None
    return (int(match.group(1)), match.group(2))


def parse_part(roman: str, part: str, default_schema: str, table: Element) -> list[Row]:
    ctx = f"{roman}.{part}"
    rows: list[Row] = []
    trs = xpath_elements(table, ".//tr")
    if not trs:
        raise ParseError(f"{ctx}: table has no rows")
    first = tuple(
        clean(element_text(td), ctx) for td in xpath_elements(trs[0], "./td|./th")
    )
    if first != HEADER:
        raise ParseError(f"{ctx}: header {first} != expected {HEADER}")
    last: Row | None = None
    for tr in trs[1:]:
        cells = xpath_elements(tr, "./td|./th")
        if len(cells) == 1:
            text = " ".join(element_text(cells[0]).split())
            if (
                last is not None
                and (part, last.record_id) in SINGLE_CELL_REASON_PINS
                and not text.startswith("▼")
            ):
                last.reason = f"{last.reason} {clean(text, ctx)}"
                continue
            check_marker(text, ctx)
            continue
        if len(cells) == 4:
            shape = (
                None
                if last is None
                else FOUR_CELL_CONTINUATION_PINS.get((part, last.record_id))
            )
            if last is None or shape is None:
                raise ParseError(f"{ctx}: unpinned four-cell row")
            if shape == "date-missing":
                if cell_lines(cells[0], ctx):
                    raise ParseError(f"{ctx}: number content in four-cell row")
                continue_row(ctx, part, last, cells[1:])
            else:
                continue_row(ctx, part, last, cells)
            continue
        if len(cells) != len(HEADER):
            raise ParseError(f"{ctx}: row has {len(cells)} cells")
        lines = [cell_lines(c, ctx) for c in cells]
        if not any(lines):
            continue
        number_lines = lines[0]
        if not number_lines:
            if last is None:
                raise ParseError(f"{ctx}: continuation row before first entry")
            continue_row(ctx, part, last, cells[1:])
            continue
        if len(number_lines) != 1:
            raise ParseError(f"{ctx}: {len(number_lines)} lines in number cell")
        printed_number = number_lines[0]
        if (part, printed_number) in NUMBER_PERIOD_PINS:
            printed_number = f"{printed_number}."
        match = NUMBER_RE.match(printed_number)
        if match is None:
            raise ParseError(f"{ctx}: unrecognized entry number {number_lines[0]!r}")
        record_id = match.group(1) + match.group(2)
        entry_ctx = f"{ctx} entry {record_id}"
        row = Row(f"{roman}.{part}", default_schema, MEASURE, record_id=record_id)
        parse_name(entry_ctx, part, record_id, cells[1], row)
        parse_info(entry_ctx, part, record_id, cells[2], row)
        row.reason = " ".join(cell_lines(cells[3], entry_ctx))
        parse_date_cell(entry_ctx, part, record_id, cells[4], row)
        rows.append(row)
        last = row
    keys = [sort_key(r.record_id) for r in rows]
    if keys != sorted(keys) or len(set(keys)) != len(keys):
        raise ParseError(f"{ctx}: entry numbers not strictly increasing")
    # Schema is decided once continuations have landed: Company-only
    # identifier columns force the schema.
    for row in rows:
        row.schema = entry_schema(part, default_schema, row)
    return rows


def parse_annex_i(roman: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    part_index = -1
    part_tables = [0 for _ in PARTS]
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
        if child.tag == "p" and cls == "title-gr-seq-level-1":
            heading = clean(element_text(child), roman)
            if heading != SUBTITLE:
                raise ParseError(f"{roman}: unexpected subtitle {heading!r}")
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-2":
            part_index += 1
            if part_index >= len(PARTS):
                raise ParseError(f"{roman}: more part headings than parts")
            heading = clean(element_text(child), roman)
            if heading != PARTS[part_index][0]:
                raise ParseError(f"{roman}: unexpected part heading {heading!r}")
            continue
        if child.tag == "div" and cls == "centered":
            if part_index < 0:
                raise ParseError(f"{roman}: table before first part heading")
            _, part, schema = PARTS[part_index]
            part_tables[part_index] += 1
            table = xpath_elements(child, ".//table", expect_exactly=1)[0]
            rows.extend(parse_part(roman, part, schema, table))
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if part_tables != [1 for _ in PARTS]:
        raise ParseError(f"{roman}: part table counts {part_tables}, expected one each")
    return rows


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    for roman, block in annex_blocks(doc, {"I"} | NON_TARGET):
        if roman in NON_TARGET:
            continue
        annex_rows = parse_annex_i(roman, block)
        if not annex_rows:
            raise ParseError(f"{roman}: no entries extracted")
        rows.extend(annex_rows)
    return rows


@click.command(help="Parse consolidated Regulation 269/2014 into a CSV candidate.")
@click.argument("celex")
@click.option(
    "--source",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Parse these exact XHTML bytes instead of fetching from CELLAR.",
)
def main(celex: str, source: Path | None) -> None:
    try:
        check_registry(
            PROGRAM_KEY, [MEASURE], [part[2] for part in PARTS] + ["Company"]
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
