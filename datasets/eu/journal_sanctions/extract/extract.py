# type: ignore
"""Turn an EU Official Journal sanctions notice into spreadsheet rows.

This is the data-entry aid for the `eu_journal_sanctions` source spreadsheet: a
maintainer adds newly published Council notices by hand, and this tool does the
tedious first pass — it reads the notice's HTML, finds the "Persons"/"Entities"
annex tables and maps each entry onto the columns of the "Unconsolidated" tab so
the rows can be pasted in (and copied to "Context"). It does NOT touch the sheet
or the crawler pipeline; a human reviews and pastes the output.

Usage:
    python datasets/eu/journal_sanctions/extract/extract.py 32026D1364

The notice HTML is fetched from the Publications Office CELLAR repository
(publications.europa.eu) by content negotiation on the CELEX id. CELLAR serves
the same rendered XHTML as the EUR-Lex front end but, unlike eur-lex.europa.eu,
is not behind the bot-challenge WAF, so a plain request works.

The label-to-column mapping below only covers fields seen so far. Anything it
doesn't recognise is preserved verbatim in a trailing "[...]" block in Notes and
reported in the run summary, so gaps are visible rather than silently dropped.
"""

import csv
import re
from pathlib import Path

import click
import requests
from lxml import html
from lxml.html import HtmlElement
from normality import slugify, squash_spaces

from zavod import helpers as h

OUT_DIR = Path(__file__).parent / "out"
SOURCE_URL = "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A{celex}"
# CELLAR content negotiation: resolves a CELEX to its English XHTML rendering.
CELLAR_URL = "http://publications.europa.eu/resource/celex/{celex}"
CELLAR_HEADERS = {"Accept": "application/xhtml+xml", "Accept-Language": "eng"}

# Column order of the "Unconsolidated" / "Context" tabs.
SHEET_COLUMNS = [
    "List ID",
    "Type",
    "Name",
    "Alias",
    "Position",
    "DOB",
    "POB",
    "Country",
    "passport",
    "taxNumber",
    "idNumber",
    "kppCode",
    "registrationNumber",
    "imoNumber",
    "Address",
    "Gender",
    "Source URL",
    "Notes",
    "related",
    "startDate",
    "crypto wallet",
    "website",
    "email",
    "previousName",
    "weakAlias",
]

# Nationality is published as a demonym ("Russian") but the Country column wants a
# territory name ("Russia"). Only the demonyms actually seen are mapped; an
# unmapped demonym passes through unchanged for the maintainer to fix.
DEMONYMS = {
    "russian": "Russia",
    "azerbaijani": "Azerbaijan",
    "israeli": "Israel",
    "ukrainian": "Ukraine",
    "belarusian": "Belarus",
    "kazakh": "Kazakhstan",
    "chinese": "China",
    "latvian": "Latvia",
    "lithuanian": "Lithuania",
    "estonian": "Estonia",
    "moldovan": "Moldova",
    "georgian": "Georgia",
    "armenian": "Armenia",
    "serbian": "Serbia",
    "syrian": "Syria",
    "iranian": "Iran",
    "turkish": "Türkiye",
    "uzbek": "Uzbekistan",
    "tajik": "Tajikistan",
    "kyrgyz": "Kyrgyzstan",
    "turkmen": "Turkmenistan",
    "german": "Germany",
    "french": "France",
    "british": "United Kingdom",
    "american": "United States",
}

# Slugified identifying-info labels that copy straight into a single column.
SIMPLE_LABELS = {
    "function": "Position",
    "position": "Position",
    "pob": "POB",
    "gender": "Gender",
    "imo": "imoNumber",
    "email": "email",
}
# Labels whose values accumulate (multiple entries) into one column.
TAX_LABELS = {
    "inn",
    "inn_russian_tax_id",
    "tax_identification_number",
    "taxpayer_identification_number_inn",
    "national_tax_id",
    "tin",
}
REG_LABELS = {
    "ogrn",
    "primary_state_identification_number_ogrn",
    "registration_number",
}
KPP_LABELS = {"kpp", "ppc"}
# Personal/company id numbers without a dedicated column of their own.
ID_LABELS = {"okpo", "snils"}
# Website fields; a notice may list several (e.g. Russian and international).
WEBSITE_LABELS = {"web", "website", "russian_website", "international_website"}
# Both a person's date of birth and an entity's incorporation date go in DOB,
# matching how the existing Russia entity rows in the sheet are filled.
DOB_LABELS = {"dob", "date_of_registration"}

# Identifying-info labels that only ever describe a natural person; their
# presence is what tells a person row from an entity row when a notice has no
# "Persons"/"Entities" heading above its tables.
PERSON_SIGNALS = {"function", "position", "dob", "pob", "nationality", "gender"}

# Markers used to schema-type entities. "Type of entity" is most reliable; when
# it is absent the name is matched against these, falling back to LegalEntity.
ORG_KEYWORDS = ("non-profit", "organisation", "institution", "enterprise")
COMPANY_NAME_MARKERS = (
    "llc",
    "ltd",
    "limited",
    "company",
    "joint stock",
    "joint-stock",
    "ooo",
    "oao",
    "pao",
    "zao",
    "gmbh",
    "corporation",
    "plc",
)
ORG_NAME_MARKERS = (
    "foundation",
    "institute",
    "institution",
    "agency",
    "ministry",
    "service",
    "centre",
    "center",
    "university",
    "school",
    "association",
    "union",
    "fund",
    "federal state",
    "committee",
    "council",
    "directorate",
    "technopolis",
)

# The reasons column is headed inconsistently across notices ("Reasons" vs
# "Statement of reasons"); the first of these present supplies Notes.
REASON_COLUMNS = ("reasons", "statement_of_reasons")
# Fields whose value can run onto a second, colon-less line in the source.
WRAPPABLE_LABELS = {"function", "address", "pob"}
# Colon-less lines starting like these are contact details with no column.
TELECOM = re.compile(r"^(tel|fax|phone|e-?mail)\b", re.IGNORECASE)


def multiline_squash(text: str) -> str:
    """Collapse runs of spaces on each line while keeping line breaks."""
    return "\n".join(squash_spaces(line) for line in text.split("\n")).strip()


def fetch_from_cellar(celex: str) -> HtmlElement:
    """Fetch a notice's English XHTML rendering from CELLAR by CELEX id."""
    response = requests.get(CELLAR_URL.format(celex=celex), headers=CELLAR_HEADERS)
    response.raise_for_status()
    return html.fromstring(response.content)


def categorized_tables(doc: HtmlElement) -> list[tuple[str | None, HtmlElement]]:
    """Pair each annex table with the "Persons"/"Entities" heading above it.

    The headings are plain spans preceding their table in document order, so we
    walk the tree once and remember the last heading seen. A table with no
    heading above it keeps None and is classified later from its content.
    """
    tables = set(doc.xpath("//table[contains(@class, 'oj-table')]"))
    current: str | None = None
    result: list[tuple[str | None, HtmlElement]] = []
    for element in doc.iter():
        text = (element.text or "").strip()
        if text in ("Persons", "Entities"):
            current = text
        if element in tables:
            result.append((current, element))
    return result


def split_name(cell_text: str) -> tuple[str, str, str]:
    """Split a Name cell into (name, aliases, previous names).

    The first line is the primary name; following lines are "a.k.a." aliases,
    "f.k.a." former names, or script variants prefixed with a language
    ("Russian: ..."). Prefixes are stripped and values joined with "; ".
    """
    lines = [line.strip() for line in cell_text.split("\n") if line.strip()]
    if not lines:
        return "", "", ""
    aliases: list[str] = []
    previous: list[str] = []
    for line in lines[1:]:
        if re.match(r"f\.k\.a\.", line, re.IGNORECASE):
            previous.append(re.sub(r"^f\.k\.a\.\s*", "", line, flags=re.IGNORECASE))
            continue
        line = re.sub(r"^a\.k\.a\.\s*", "", line, flags=re.IGNORECASE)
        line = re.sub(r"^[A-Z][a-z]+:\s*", "", line)  # "Russian:" etc.
        line = re.sub(r"^\((.*)\)$", r"\1", line.strip())  # "(Антон ТРЕГУБ)"
        if line:
            aliases.append(line.strip())
    return lines[0], "; ".join(aliases), "; ".join(previous)


def parse_identifying(cell_text: str) -> tuple[list[tuple[str, str]], list[str]]:
    """Parse an "Identifying information" cell into (label, value) pairs.

    Each line is "Label: value"; the label is slugified for matching. A line
    without a colon either continues the previous value when that field is one
    that wraps in the source (a Function/Address/place can run onto a second
    line) or, otherwise — including phone/fax lines and stray colon-less fields —
    becomes a free-text leftover appended to Notes. This keeps wrapped positions
    intact without letting a phone number leak into the field above it.
    """
    pairs: list[list[str]] = []
    leftovers: list[str] = []
    for line in cell_text.split("\n"):
        line = line.strip()
        if not line:
            continue
        if ":" in line:
            label, value = line.split(":", 1)
            pairs.append([slugify(label, sep="_") or "", value.strip()])
        elif TELECOM.match(line):
            leftovers.append(line)
        elif pairs and pairs[-1][0] in WRAPPABLE_LABELS:
            pairs[-1][1] = f"{pairs[-1][1]} {line}".strip()
        else:
            leftovers.append(line)
    return [(label, value) for label, value in pairs], leftovers


def map_country(value: str) -> str:
    """Map one or more demonyms/territory names to Country-column values."""
    parts = [p.strip() for p in re.split(r"[;,]", value) if p.strip()]
    return "; ".join(DEMONYMS.get(p.lower(), p) for p in parts)


def country_value(value: str) -> str:
    """Reduce a "city, region, country" location to its trailing country part.

    Place-of-business and place-of-registration fields often carry a full
    locality; the Country column wants only the country at the end.
    """
    parts = [p.strip() for p in value.split(",") if p.strip()]
    return parts[-1] if parts else value.strip()


def fix_url(value: str) -> str:
    """Ensure a Web value carries a scheme so it cleans as a URL."""
    value = value.strip()
    if value and not re.match(r"^https?://", value):
        return "https://" + value
    return value


def country_from_address(address: str) -> str:
    """Fall back to the trailing component of an address as the country.

    Used only when no explicit place-of-business/registration field is present.
    Skips components containing digits (postal codes) to avoid false positives.
    """
    parts = [p.strip() for p in address.split(",") if p.strip()]
    if parts and not any(ch.isdigit() for ch in parts[-1]):
        return parts[-1]
    return ""


def schema_for_entity(type_of_entity: str, name: str) -> str:
    """Pick a FollowTheMoney schema for an annex entity.

    Prefers the stated "Type of entity"; when absent, reads the legal form from
    the name. Falls back to the neutral LegalEntity rather than guessing, so an
    unrecognised form surfaces for the maintainer instead of asserting a type.
    """
    typed = type_of_entity.lower()
    if "company" in typed:
        return "Company"
    if typed and any(keyword in typed for keyword in ORG_KEYWORDS):
        return "Organization"
    named = name.lower()
    if any(marker in named for marker in COMPANY_NAME_MARKERS):
        return "Company"
    if any(marker in named for marker in ORG_NAME_MARKERS):
        return "Organization"
    return "LegalEntity"


def row_is_person(cells: dict[str, HtmlElement]) -> bool:
    """Classify a heading-less annex row as a person from its detail fields."""
    cell = cells.get("identifying_information")
    text = multiline_squash(cell.text_content()) if cell is not None else ""
    pairs, _ = parse_identifying(text)
    return any(label in PERSON_SIGNALS for label, _ in pairs)


def build_row(
    cells: dict[str, HtmlElement], is_person: bool, celex: str, unmapped: set[str]
) -> dict[str, str]:
    """Map one annex table row onto the spreadsheet columns."""
    text = {key: multiline_squash(cell.text_content()) for key, cell in cells.items()}
    row = {column: "" for column in SHEET_COLUMNS}

    entry_number = text.get("column_0", "").strip("‘'’. ").strip()
    name, alias, previous = split_name(text.get("name", ""))
    row["List ID"] = entry_number
    row["Name"] = name
    row["Alias"] = alias
    row["previousName"] = previous
    row["Source URL"] = SOURCE_URL.format(celex=celex)
    reasons = next((text[col] for col in REASON_COLUMNS if text.get(col)), "")
    row["Notes"] = re.sub(r"\s*\n\s*", " ", reasons).strip()
    row["startDate"] = text.get("date_of_listing", "").strip("‘'’ ").strip()

    pairs, leftovers = parse_identifying(text.get("identifying_information", ""))
    tax: list[str] = []
    registration: list[str] = []
    passports: list[str] = []
    related: list[str] = []
    ids: list[str] = []
    websites: list[str] = []
    place_of_registration = ""
    type_of_entity = ""
    for label, value in pairs:
        if label == "address":
            row["Address"] = value.strip(" ,")
        elif label in SIMPLE_LABELS:
            row[SIMPLE_LABELS[label]] = value
        elif label in DOB_LABELS:
            row["DOB"] = value
        elif label == "nationality":
            row["Country"] = map_country(value)
        elif label == "principal_place_of_business":
            row["Country"] = row["Country"] or country_value(value)
        elif label == "place_of_registration":
            place_of_registration = country_value(value)
        elif label in TAX_LABELS:
            tax.append(value)
        elif label in REG_LABELS:
            registration.append(value)
        elif label in KPP_LABELS:
            row["kppCode"] = value
        elif label in ID_LABELS:
            ids.append(value)
        elif label in WEBSITE_LABELS:
            websites.append(fix_url(value))
        elif "passport" in label:
            passports.extend(p.strip() for p in re.split(r"[;,]", value) if p.strip())
        elif label.startswith("associated"):
            related.append(value)
        elif label == "type_of_entity":
            type_of_entity = value
        else:
            unmapped.add(label)
            leftovers.append(f"{label.replace('_', ' ')}: {value}")

    if not row["Country"]:
        row["Country"] = place_of_registration or country_from_address(row["Address"])
    row["taxNumber"] = "; ".join(tax)
    row["registrationNumber"] = "; ".join(registration)
    row["passport"] = "; ".join(passports)
    row["idNumber"] = "; ".join(ids)
    row["website"] = "; ".join(websites)
    row["related"] = "; ".join(related)
    row["Type"] = "Person" if is_person else schema_for_entity(type_of_entity, name)
    if leftovers:
        row["Notes"] = f"{row['Notes']}  [{'; '.join(leftovers)}]".strip()
    return row


@click.command()
@click.argument("celex", type=str)
def extract(celex: str) -> None:
    """Extract a notice's annex into an Unconsolidated-tab CSV under extract/out/.

    CELEX is the document id, e.g. 32026D1364.
    """
    doc = fetch_from_cellar(celex)

    rows: list[dict[str, str]] = []
    unmapped: set[str] = set()
    counts: dict[str, int] = {}
    for category, table in categorized_tables(doc):
        table_rows = list(
            h.parse_html_table(table, header_tag="td", index_empty_headers=True)
        )
        if not table_rows:
            continue
        for cells in table_rows:
            # Trust the heading when present; otherwise classify each row from
            # its own detail fields (a notice may table persons and entities
            # separately with no heading at all).
            if category == "Persons":
                is_person = True
            elif category == "Entities":
                is_person = False
            else:
                is_person = row_is_person(cells)
            rows.append(build_row(cells, is_person, celex, unmapped))
            label = "Persons" if is_person else "Entities"
            counts[label] = counts.get(label, 0) + 1

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / f"{celex}.csv"
    with out_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=SHEET_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    click.echo(f"Wrote {len(rows)} rows to {out_path}")
    for category, count in counts.items():
        click.echo(f"  {category}: {count}")
    if unmapped:
        click.echo(
            "Unmapped identifying-info labels (kept in Notes): "
            + ", ".join(sorted(unmapped))
        )
    click.echo(
        "Check that every annex table and any free-text changes were captured, "
        "then paste the rows into the Unconsolidated and Context tabs."
    )


if __name__ == "__main__":
    extract()
