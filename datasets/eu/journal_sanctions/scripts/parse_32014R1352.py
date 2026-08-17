"""Parse consolidated Regulation (EU) 1352/2014 (Yemen) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's target annex:

- Annex I — persons and entities designated under the UN Yemen sanctions
  regime (UNSCR 2140, Articles 1a and 2), parts A. PERSONS and B. ENTITIES.
  Person entries are grid-list rows: a name paragraph with an
  "(aliases: (a) …; (b) …)" parenthetical, an optional "Original script:"
  line, one run-on paragraph of labelled UN fields, then the narrative
  sentinel and the Sanctions Committee's summary, which is the reason for
  listing. The single entity entry prints as annex-level paragraphs.
  Travel bans live in Decision 2014/932/CFSP.

Annex II lists competent-authority websites, not designations. Delisted
entries leave numbering gaps. Dates are transcribed as the source prints
them ("25.2.2021", "26 Sep. 2022"); amendment-history parentheticals after
a designation date are stripped; the crawler normalizes dates.

Deliberate drops, no contract column: "Original script:" renderings printed
only as images (untranscribable; text renderings become aliases); the
entity heading's "( 1 )" reference to the Article 2 exemption footnote;
"n/a" placeholders; and, inside the narrative, the Committee's boilerplate
(the section 5(h) sentence and the "Date on which the narrative summary
became available" line), the "Additional information:" sub-heading, and the
source-citation bullets closing some narratives (with or without their
"Verification of …" sub-heading) — the UN's evidentiary references, not
designation content.

Output: data/consolidated/32014R1352.csv (the EU Journal consolidated CSV
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
    ParseError,
    Row,
    annex_blocks,
    annex_id,
    check_consolidated_celex,
    check_marker,
    check_registry,
    clean,
    load_source,
    summary,
    to_record,
    validate_records,
    verbatim_date,
    write_csv,
)
from lxml import html
from zavod.helpers.html import element_text, xpath_elements
from zavod.util import Element

FRAMEWORK_CELEX = "32014R1352"
PROGRAM_KEY = "EU-YEM"
# The regulation implements the Article 2 fund freeze; travel bans live in
# Decision 2014/932/CFSP.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"II"})
SUBTITLE = "LIST OF PERSONS, ENTITIES AND BODIES REFERRED TO IN ARTICLES 1A AND 2"
PART_HEADINGS = ("A. PERSONS", "B. ENTITIES")

# Part A entries are all natural persons; part B entries are classified per
# reviewed entry (the Houthis are an armed movement, not a legal person).
# A new entity number breaks the run for schema review.
B_ENTRY_SCHEMAS = {"1": "Organization"}

NUMBER_RE = re.compile(r"^(\d+)\.$")
B_HEADING_RE = re.compile(r"^(\d+)\.\s+(.+)$")
# The entity heading carries a reference to the footnote "Article 2 shall
# not apply to this entity" — an exemption qualifier with no contract
# column, deliberately not transcribed.
FOOTNOTE_REF_RE = re.compile(r" \( \d+ \)")
NAME_ALIAS_RE = re.compile(r"^(.+?) \((alias|aliases): (.+)\)$")
ALIAS_PIECE_RE = re.compile(r"^\(([a-z])\)\s*(.+)$")
LETTER_ITEM_RE = re.compile(r"([a-z])\) ")
# Amendment-history parentheticals after a designation date are stripped
# per the contract's startDate rule.
AMENDED_RE = re.compile(r" \(amended on [^)]*\)$")

ORIGINAL_SCRIPT_LABEL = "Original script:"
# Entries whose original-script rendering is printed only as an image;
# there is nothing to transcribe. Text renderings become aliases.
IMAGE_SCRIPT_PINS = frozenset({("A", "1"), ("A", "2")})

SENTINEL = (
    "Additional information from the narrative summary of reasons for "
    "listing provided by the Sanctions Committee:"
)
# Committee boilerplate inside the narrative, skipped verbatim (the
# guidelines sentence prints in 5(g)/5(h) capitalisation variants, with and
# without the Oxford comma).
P5H_SENTENCES = frozenset(
    {
        "In accordance with Section 5(g) of its Guidelines, the Security"
        " Council Committee established pursuant to Resolution 2140 makes"
        " accessible a narrative summary of reasons for the listing for"
        " individuals, groups, undertakings and entities included in its"
        " sanctions list.",
        "In accordance with section 5(h) of its Guidelines, the Security"
        " Council Committee established pursuant to resolution 2140 makes"
        " accessible a narrative summary of reasons for the listing for"
        " individuals, groups, undertakings and entities included in its"
        " sanctions list.",
        "In accordance with section 5(h) of its Guidelines, the Security"
        " Council Committee established pursuant to resolution 2140 makes"
        " accessible a narrative summary of reasons for the listing for"
        " individuals, groups, undertakings, and entities included in its"
        " sanctions list.",
    }
)
DATE_AVAILABLE_PREFIX = (
    "Date on which the narrative summary became available on the Committee’s website:"
)
SKIP_HEADINGS = frozenset({"Additional information:"})
VERIFICATION_HEADINGS = frozenset(
    {
        "Verification of active military role:",
        "Verification of role in human rights abuses:",
    }
)

# Run-on field labels, sliced wherever they occur in the field paragraph.
FIELD_COLUMNS = {
    "Title": "position",
    "Designation": "position",
    "Address": "address",
    "Date of Birth": "birthDate",
    "DOB": "birthDate",
    "Place of Birth": "birthPlace",
    "POB": "birthPlace",
    "Nationality": "nationality",
    "Passport no": "passportNumber",
    "National identification no": "idNumber",
    "Good quality a.k.a.": "alias",
    "Low quality a.k.a.": "weakAlias",
}
DATE_LABELS = frozenset({"Listed on", "Date of UN designation"})
OTHER_INFORMATION = "Other information"
ALL_LABELS = frozenset(FIELD_COLUMNS) | DATE_LABELS | {OTHER_INFORMATION}
LABEL_RE = re.compile(
    r"(?:(?<=^)|(?<=[ .;]))("
    + "|".join(re.escape(label) for label in sorted(ALL_LABELS, key=len, reverse=True))
    + r"): "
)
NOT_AVAILABLE = frozenset({"n/a"})

# Reviewed decompositions of "Other information" values that mix prose with
# printed sub-labels (Gender, Physical Description) and INTERPOL notices,
# keyed by (part, entry) with the exact printed value. A value change or an
# unreviewed value containing ":" breaks the run for re-review; values
# without ":" are plain prose and become one bare notes value.
OTHER_INFO_OVERRIDES: dict[tuple[str, str], tuple[str, tuple[tuple[str, str], ...]]]
OTHER_INFO_OVERRIDES = {
    ("A", "1"): (
        "Gender: male. INTERPOL-UN Security Council Special Notice web link:"
        " https://www.interpol.int/en/notice/search/un/5837273.",
        (
            ("gender", "male"),
            (
                "notes",
                "INTERPOL-UN Security Council Special Notice web link:"
                " https://www.interpol.int/en/notice/search/un/5837273.",
            ),
        ),
    ),
    ("A", "2"): (
        "Gender: Male. INTERPOL-UN Security Council Special Notice web link:"
        " https://www.interpol.int/en/notice/search/un/5837297.",
        (
            ("gender", "Male"),
            (
                "notes",
                "INTERPOL-UN Security Council Special Notice web link:"
                " https://www.interpol.int/en/notice/search/un/5837297.",
            ),
        ),
    ),
    ("A", "7"): (
        "As Houthi ‘Assistant Minister of Defence for Logistics’, assisted"
        " the Houthis in acquiring smuggled arms and weapons. As ‘Judicial"
        " Custodian’ directly involved in the widespread and unlawful"
        " appropriation of assets and entities owned by private individuals"
        " under arrest by the Houthis or forced to take refuge outside of"
        " Yemen. Physical Description: Eye Colour: Brown; Hair: Grey;"
        " Complexion: Medium; Build: Slim; Height (ft/in): Unknown; Weight"
        " (lbs): Unknown; and Clan: Member of the Hashid tribal confederacy."
        " Photograph available for inclusion in INTERPOL-UNSC Special Notice"
        " web link: INTERPOL-UN Security Council Special Notice web link:"
        " https://www.interpol.int/en/How-we-work/Notices/View-UN-Notices-Individuals.",
        (
            (
                "notes",
                "As Houthi ‘Assistant Minister of Defence for Logistics’,"
                " assisted the Houthis in acquiring smuggled arms and"
                " weapons. As ‘Judicial Custodian’ directly involved in the"
                " widespread and unlawful appropriation of assets and"
                " entities owned by private individuals under arrest by the"
                " Houthis or forced to take refuge outside of Yemen.",
            ),
            (
                "appearance",
                "Eye Colour: Brown; Hair: Grey; Complexion: Medium; Build:"
                " Slim; Height (ft/in): Unknown; Weight (lbs): Unknown; and"
                " Clan: Member of the Hashid tribal confederacy.",
            ),
            (
                "notes",
                "Photograph available for inclusion in INTERPOL-UNSC Special"
                " Notice web link: INTERPOL-UN Security Council Special"
                " Notice web link:"
                " https://www.interpol.int/en/How-we-work/Notices/View-UN-Notices-Individuals.",
            ),
        ),
    ),
    ("A", "8"): (
        "Houthi Military Chief of General Staff, plays the leading role in"
        " orchestrating the Houthis’ military efforts that are directly"
        " threatening the peace, security and stability of Yemen, including"
        " in Marib, as well as cross-border attacks against Saudi Arabia."
        " Photograph available for inclusion in INTERPOL-UN Security Council"
        " Special Notice web link: INTERPOL-UN Security Council Special"
        " Notice web link:"
        " https://www.interpol.int/en/How-we-work/Notices/View-UN-Notices-Individuals.",
        (
            (
                "notes",
                "Houthi Military Chief of General Staff, plays the leading"
                " role in orchestrating the Houthis’ military efforts that"
                " are directly threatening the peace, security and stability"
                " of Yemen, including in Marib, as well as cross-border"
                " attacks against Saudi Arabia.",
            ),
            (
                "notes",
                "Photograph available for inclusion in INTERPOL-UN Security"
                " Council Special Notice web link: INTERPOL-UN Security"
                " Council Special Notice web link:"
                " https://www.interpol.int/en/How-we-work/Notices/View-UN-Notices-Individuals.",
            ),
        ),
    ),
    ("A", "9"): (
        "A prominent leader of Houthi forces and commander of forces in"
        " Hudaydah, Hajjah, Al Mahwit, and Raymah, Yemen – threatening the"
        " peace, security, and stability of Yemen. As of 2021, Al-Madani was"
        " assigned to the offensive targeting Marib. Photograph available"
        " for inclusion in INTERPOL-UN Security Council Special Notice web"
        " link: INTERPOL-UN Security Council Special Notice web link:"
        " https://www.interpol.int/en/How-we-work/Notices/View-UN-Notices-Individuals.",
        (
            (
                "notes",
                "A prominent leader of Houthi forces and commander of forces"
                " in Hudaydah, Hajjah, Al Mahwit, and Raymah, Yemen –"
                " threatening the peace, security, and stability of Yemen."
                " As of 2021, Al-Madani was assigned to the offensive"
                " targeting Marib.",
            ),
            (
                "notes",
                "Photograph available for inclusion in INTERPOL-UN Security"
                " Council Special Notice web link: INTERPOL-UN Security"
                " Council Special Notice web link:"
                " https://www.interpol.int/en/How-we-work/Notices/View-UN-Notices-Individuals.",
            ),
        ),
    ),
    ("A", "10"): (
        "Houthi Naval Forces Chief of Staff, who has masterminded lethal"
        " attacks against international shipping in the Red Sea, plays a"
        " leading role in Houthi naval efforts that directly threaten the"
        " peace, security, and stability of Yemen. Physical Description: Eye"
        " Color: Brown; Hair: Brown. INTERPOL-UN Security Council Special"
        " Notice web link:"
        " https://www.interpol.int/en/How-we-work/Notices/View-UN-Notices-Individual",
        (
            (
                "notes",
                "Houthi Naval Forces Chief of Staff, who has masterminded"
                " lethal attacks against international shipping in the Red"
                " Sea, plays a leading role in Houthi naval efforts that"
                " directly threaten the peace, security, and stability of"
                " Yemen.",
            ),
            ("appearance", "Eye Color: Brown; Hair: Brown."),
            (
                "notes",
                "INTERPOL-UN Security Council Special Notice web link:"
                " https://www.interpol.int/en/How-we-work/Notices/View-UN-Notices-Individual",
            ),
        ),
    ),
    ("A", "11"): (
        "Former Deputy Head of the Houthi National Security Bureau (NSB),"
        " oversaw detainees of the NSB who were subjected to torture and"
        " other mistreatment while detained, planned and directed illegal"
        " arrests and detention of humanitarian workers and the unlawful"
        " diversion of humanitarian assistance in violation of international"
        " humanitarian law. Physical Description: Eye Color: Brown; Hair:"
        " Brown. INTERPOL-UN Security Council Special Notice web link:"
        " https://www.interpol.int/en/How-we-work/Notices/View-UN-Notices-Individual",
        (
            (
                "notes",
                "Former Deputy Head of the Houthi National Security Bureau"
                " (NSB), oversaw detainees of the NSB who were subjected to"
                " torture and other mistreatment while detained, planned and"
                " directed illegal arrests and detention of humanitarian"
                " workers and the unlawful diversion of humanitarian"
                " assistance in violation of international humanitarian"
                " law.",
            ),
            ("appearance", "Eye Color: Brown; Hair: Brown."),
            (
                "notes",
                "INTERPOL-UN Security Council Special Notice web link:"
                " https://www.interpol.int/en/How-we-work/Notices/View-UN-Notices-Individual",
            ),
        ),
    ),
    ("A", "12"): (
        "Ahmad al-Hamzi, the commander of the Houthi Air Force and Air"
        " Defense Forces, as well as its UAV program, plays a leading role"
        " in Houthi military efforts that directly threaten the peace,"
        " security, and stability of Yemen. Physical Description: Eye Color:"
        " Brown; Hair: Brown. INTERPOL-UN Security Council Special Notice"
        " web link:"
        " https://www.interpol.int/en/How-we-work/Notices/View-UN-Notices-Individuals",
        (
            (
                "notes",
                "Ahmad al-Hamzi, the commander of the Houthi Air Force and"
                " Air Defense Forces, as well as its UAV program, plays a"
                " leading role in Houthi military efforts that directly"
                " threaten the peace, security, and stability of Yemen.",
            ),
            ("appearance", "Eye Color: Brown; Hair: Brown."),
            (
                "notes",
                "INTERPOL-UN Security Council Special Notice web link:"
                " https://www.interpol.int/en/How-we-work/Notices/View-UN-Notices-Individuals",
            ),
        ),
    ),
}


# Formats observed in this document: dotted dates ("25.2.2021") and UN
# abbreviated dates ("26 Sep. 2022").
DATE_FORMATS = (
    "dotted",
    "abbrev",
)


def split_lettered(ctx: str, value: str) -> list[str]:
    """Split a UN lettered enumeration ("a) X; b) Y" or "a) X b) Y")."""
    if not value.startswith("a) "):
        return [value]
    # Sequential scan: an item marker counts only at paren depth zero and
    # after a boundary, and its own ")" is not a closing paren.
    starts: list[tuple[str, int, int]] = []
    depth = 0
    index = 0
    while index < len(value):
        match = LETTER_ITEM_RE.match(value, index)
        boundary = index == 0 or value[index - 1] in " ;"
        if match is not None and depth == 0 and boundary:
            starts.append((match.group(1), index, match.end()))
            index = match.end()
            continue
        if value[index] == "(":
            depth += 1
        elif value[index] == ")" and depth > 0:
            depth -= 1
        index += 1
    letters = [letter for letter, _, _ in starts]
    expected = [chr(ord("a") + i) for i in range(len(starts))]
    if letters != expected:
        raise ParseError(f"{ctx}: broken enumeration sequence in {value[:60]!r}")
    items: list[str] = []
    for index, (_, _, body_start) in enumerate(starts):
        end = starts[index + 1][1] if index + 1 < len(starts) else len(value)
        items.append(value[body_start:end].strip().rstrip(";").strip())
    return items


def field_values(ctx: str, value: str) -> list[str]:
    """Expand a structured field into values; n/a means absent."""
    value = value.strip()
    if value.endswith("."):
        value = value[:-1].strip()
    if value in NOT_AVAILABLE:
        return []
    return [item for item in split_lettered(ctx, value) if item not in NOT_AVAILABLE]


def parse_aliases(ctx: str, kind: str, body: str) -> list[str]:
    if kind == "alias":
        return [body]
    pieces = body.split("; ")
    aliases: list[str] = []
    for index, piece in enumerate(pieces):
        match = ALIAS_PIECE_RE.match(piece)
        if match is None:
            raise ParseError(f"{ctx}: unrecognized alias piece {piece!r}")
        if match.group(1) != chr(ord("a") + index):
            raise ParseError(f"{ctx}: broken alias sequence in {body[:60]!r}")
        aliases.append(match.group(2))
    return aliases


def parse_name(ctx: str, text: str, row: Row) -> None:
    text = text.strip()
    if text.endswith(")."):
        text = text[:-1]
    match = NAME_ALIAS_RE.match(text)
    if match is not None:
        name, kind, body = match.groups()
        row.add("alias", parse_aliases(ctx, kind, body))
        text = name
    if "(alias" in text:
        raise ParseError(f"{ctx}: unextracted alias in name {text[:60]!r}")
    row.add("name", [text])


def apply_other_information(
    ctx: str, part: str, record_id: str, value: str, row: Row
) -> None:
    override = OTHER_INFO_OVERRIDES.get((part, record_id))
    if override is not None:
        expected, mapped = override
        if value != expected:
            raise ParseError(f"{ctx}: Other information changed, re-review")
        for column, mapped_value in mapped:
            row.add(column, [mapped_value])
        return
    if ":" in value:
        raise ParseError(f"{ctx}: unreviewed Other information with sub-labels")
    row.add("notes", [value])


def set_start_date(ctx: str, raw: str, row: Row) -> None:
    if row.start_date:
        raise ParseError(f"{ctx}: second designation date")
    value = raw.strip()
    if value.endswith("."):
        value = value[:-1].strip()
    value = AMENDED_RE.sub("", value).strip()
    row.start_date = verbatim_date(value, ctx, DATE_FORMATS)


def parse_field_blob(ctx: str, part: str, record_id: str, blob: str, row: Row) -> None:
    matches = list(LABEL_RE.finditer(blob))
    if not matches or matches[0].start() != 0:
        raise ParseError(f"{ctx}: field paragraph does not open with a label")
    for index, match in enumerate(matches):
        label = match.group(1)
        end = matches[index + 1].start() if index + 1 < len(matches) else len(blob)
        value = blob[match.end() : end].strip()
        if label in DATE_LABELS:
            set_start_date(ctx, value, row)
        elif label == OTHER_INFORMATION:
            apply_other_information(ctx, part, record_id, value, row)
        else:
            row.add(FIELD_COLUMNS[label], field_values(ctx, value))


def parse_narrative_line(ctx: str, text: str, state: dict[str, object]) -> None:
    """Route one post-sentinel paragraph: rationale, boilerplate, citations."""
    if state["citations"]:
        raise ParseError(f"{ctx}: paragraph after the citation bullets {text[:60]!r}")
    if text in P5H_SENTENCES or text.startswith(DATE_AVAILABLE_PREFIX):
        return
    if text in SKIP_HEADINGS:
        return
    if text in VERIFICATION_HEADINGS:
        state["citations"] = True
        return
    reason = state["reason"]
    assert isinstance(reason, list)
    reason.append(text)


def parse_person(roman: str, part: str, grid: Element) -> Row:
    cols = xpath_elements(grid, "./div", expect_exactly=2)
    number = clean(element_text(cols[0]), roman)
    match = NUMBER_RE.match(number)
    if match is None:
        raise ParseError(f"{roman}: unrecognized entry number {number!r}")
    record_id = match.group(1)
    ctx = f"{roman}.{part} entry {record_id}"
    row = Row(annex_id(roman, part), "Person", MEASURE, record_id=record_id)
    state: dict[str, object] = {"reason": [], "citations": False}
    seen_name = False
    seen_blob = False
    in_narrative = False
    for child in cols[1]:
        cls = child.get("class") or ""
        if child.tag == "div" and "grid-container" in cls:
            if not in_narrative:
                raise ParseError(f"{ctx}: grid bullet outside the narrative")
            # Source-citation bullets, printed after the rationale with or
            # without a "Verification of …" heading: the UN's evidentiary
            # references, deliberately not transcribed. Once they start,
            # only bullets may follow.
            state["citations"] = True
            bullet_cols = xpath_elements(child, "./div", expect_exactly=2)
            if clean(element_text(bullet_cols[0]), ctx) != "—":
                raise ParseError(f"{ctx}: unrecognized citation bullet")
            continue
        if child.tag != "p":
            raise ParseError(f"{ctx}: unexpected <{child.tag}> in entry")
        text = clean(element_text(child), ctx)
        if not seen_name:
            if cls != "norm":
                raise ParseError(f"{ctx}: entry does not open with a name line")
            parse_name(ctx, text, row)
            seen_name = True
            continue
        if cls != "list":
            raise ParseError(f"{ctx}: unexpected <p class={cls!r}> in entry")
        if in_narrative:
            parse_narrative_line(ctx, text, state)
            continue
        if text == SENTINEL:
            in_narrative = True
            continue
        if text.startswith(ORIGINAL_SCRIPT_LABEL):
            value = text[len(ORIGINAL_SCRIPT_LABEL) :].strip()
            if value == "":
                if (part, record_id) not in IMAGE_SCRIPT_PINS:
                    raise ParseError(f"{ctx}: unpinned image-only original script")
                if not child.xpath(".//img"):
                    raise ParseError(f"{ctx}: empty original-script line")
                continue
            row.add("alias", [value])
            continue
        if seen_blob:
            raise ParseError(f"{ctx}: second field paragraph {text[:60]!r}")
        parse_field_blob(ctx, part, record_id, text, row)
        seen_blob = True
    if not seen_blob:
        raise ParseError(f"{ctx}: entry has no field paragraph")
    reason = state["reason"]
    assert isinstance(reason, list)
    if not reason:
        raise ParseError(f"{ctx}: entry has no narrative reason")
    row.reason = " ".join(reason)
    if not row.start_date:
        raise ParseError(f"{ctx}: entry has no designation date")
    return row


def start_entity(roman: str, part: str, div: Element) -> Row:
    heading = clean(element_text(div), f"{roman}.{part}")
    heading = FOOTNOTE_REF_RE.sub("", heading)
    match = B_HEADING_RE.match(heading)
    if match is None:
        raise ParseError(
            f"{roman}.{part}: unrecognized entity heading {heading[:60]!r}"
        )
    record_id, body = match.groups()
    ctx = f"{roman}.{part} entry {record_id}"
    schema = B_ENTRY_SCHEMAS.get(record_id)
    if schema is None:
        raise ParseError(f"{ctx}: entity not classified, review its schema")
    row = Row(annex_id(roman, part), schema, MEASURE, record_id=record_id)
    parse_name(ctx, body, row)
    return row


def finish_entity(ctx: str, row: Row, reason: list[str]) -> None:
    if not reason:
        raise ParseError(f"{ctx}: entity has no narrative reason")
    row.reason = " ".join(reason)
    if not row.start_date:
        raise ParseError(f"{ctx}: entity has no designation date")


def parse_annex_i(roman: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    part = ""
    entity_row: Row | None = None
    entity_reason: list[str] = []
    entity_narrative = False
    for child in block:
        cls = child.get("class") or ""
        ctx = f"{roman}.{part or '?'}"
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls == "":
            if clean(element_text(child), ctx):
                raise ParseError(f"{ctx}: unexpected annex paragraph")
            continue
        if child.tag == "p" and cls == "modref":
            check_marker(" ".join(element_text(child).split()), ctx)
            continue
        text = clean(element_text(child), ctx)
        if child.tag == "p" and cls == "title-annex-1":
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-1":
            if text != SUBTITLE:
                raise ParseError(f"{roman}: annex subtitle changed")
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-2":
            if text not in PART_HEADINGS:
                raise ParseError(f"{roman}: unrecognized part heading {text!r}")
            part = text[0]
            continue
        if part == "A":
            if child.tag == "div" and "grid-container" in cls:
                rows.append(parse_person(roman, part, child))
                continue
            raise ParseError(f"{ctx}: unexpected <{child.tag} class={cls!r}> in part A")
        if part == "B":
            if child.tag == "div" and cls == "":
                if entity_row is not None:
                    finish_entity(ctx, entity_row, entity_reason)
                entity_row = start_entity(roman, part, child)
                rows.append(entity_row)
                entity_reason = []
                entity_narrative = False
                continue
            if child.tag == "p" and cls == "norm":
                if entity_row is None:
                    raise ParseError(f"{ctx}: entity body before any heading")
                ectx = f"{ctx} entry {entity_row.record_id}"
                if entity_narrative:
                    entity_reason.append(text)
                elif text == SENTINEL:
                    entity_narrative = True
                elif text.startswith("Information: "):
                    value = text[len("Information: ") :].strip()
                    entity_row.add("notes", [value])
                elif text.startswith("Date of UN designation: "):
                    raw = text[len("Date of UN designation: ") :]
                    set_start_date(ectx, raw, entity_row)
                else:
                    raise ParseError(f"{ectx}: unrecognized entity line {text[:60]!r}")
                continue
            raise ParseError(f"{ctx}: unexpected <{child.tag} class={cls!r}> in part B")
        raise ParseError(f"{roman}: content before the first part heading")
    if entity_row is not None:
        finish_entity(f"{roman}.B", entity_row, entity_reason)
    if part != "B":
        raise ParseError(f"{roman}: part sequence incomplete")
    return rows


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    for roman, block in annex_blocks(doc, {"I"} | NON_TARGET):
        if roman in NON_TARGET:
            continue
        annex_rows = parse_annex_i(roman, block)
        if not annex_rows:
            raise ParseError(f"{roman}: annex yielded no designations")
        rows.extend(annex_rows)
    return rows


@click.command()
@click.argument("celex")
@click.option(
    "--source",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Parse these exact XHTML bytes instead of fetching from CELLAR.",
)
def main(celex: str, source: Path | None) -> None:
    try:
        check_registry(
            PROGRAM_KEY, [MEASURE], {"Person"} | set(B_ENTRY_SCHEMAS.values())
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
