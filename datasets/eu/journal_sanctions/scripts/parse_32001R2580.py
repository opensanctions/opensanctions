"""Parse consolidated Regulation (EC) 2580/2001 (terrorism) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — competent authorities, not designations.
- Annex II — the Article 2(3) list of natural persons (part A) and legal
  persons, groups and entities (part B). The Council replaces this list
  wholesale on each semi-annual review, so entries are renumbered
  contiguously and print no per-designation dates. Travel measures live in
  Common Position 2001/931/CFSP, as amended by Decision (CFSP) 2026/455;
  the regulation implements the asset freeze.
- Annex III — the Article 2(6) list, currently printed only as a "[…]"
  placeholder in both parts.

Unlike the sibling regulations, the designations are prose paragraphs, not
tables: one sentence mixing the name, printed a.k.a. labels, birth data,
citizenship, identifiers and addresses in free order. There is no cell or
label structure to parse generically, so every entry is transcribed in a
reviewed table below, keyed on part and entry number and holding the exact
printed sentence next to its hand-reviewed column mapping. The parser only
verifies the document still prints exactly the reviewed sentences; any
changed, added or removed entry breaks the run for re-review. Values keep
the printed wording; the reviewed mapping performs only structural
extraction: quote unwrapping, a.k.a./abbreviation/translation structure to
aliases, "born …" clauses to birth columns, labelled identifiers to their
columns. A clause with residue no column fits (validity notes, inclusion
clauses such as "including ‘Hamas-Izz al-Din al-Qassem’") is kept verbatim
in notes, with any identifier it contains also extracted.

Part B mixes armed groups with registered legal persons under one heading
("Legal persons, groups and entities"); all its entries are emitted as
Organization.

Output: data/consolidated/32001R2580.csv (the EU Journal consolidated CSV
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
    SKIP_P_CLASSES,
    ParseError,
    Row,
    annex_blocks,
    annex_id,
    clean,
    load_source,
    single_paragraph,
    summary,
    to_record,
    validate_records,
    write_csv,
)
from followthemoney import model
from lxml import html
from zavod.helpers.html import element_text
from zavod.stateful.programs import Measure, get_program_by_key
from zavod.util import Element

FRAMEWORK_CELEX = "32001R2580"
CONSOLIDATED_RE = re.compile(r"^02001R2580-\d{8}$")
PROGRAM_KEY = "EU-TERR"
# The regulation implements the Article 2 fund freeze; travel measures live
# in Common Position 2001/931/CFSP.
MEASURE = "Asset freeze"

TARGET_ANNEX = "II"
# Annex III (the Article 2(6) list) currently prints only a placeholder in
# both parts; the parser breaks the day the Council populates it, so the
# new list shape gets reviewed.
PLACEHOLDER_ANNEX = "III"
PLACEHOLDER = "[…]"
NON_TARGET = frozenset({"I"})

# (part heading, part id, schema) in print order, for both annexes II and III.
PARTS = (
    ("A. Natural persons", "A", "Person"),
    ("B. Legal persons, groups and entities", "B", "Organization"),
)

NUMBER_RE = re.compile(r"^(\d+)\.\s+(.*)$")

# The reviewed designations: (part, entry number) → the exact printed
# sentence and its hand-reviewed column mapping. The sentence is compared
# against the document on every run; a mismatch is a re-review event, not
# a parse failure to route around.
ENTRIES: dict[tuple[str, str], tuple[str, tuple[tuple[str, str], ...]]] = {
    ("A", "1"): (
        "ABDOLLAHI Hamed (a.k.a. Mustafa Abdullahi), born 11.8.1960 in Iran."
        " Passport number: D9004878.",
        (
            ("name", "ABDOLLAHI Hamed"),
            ("alias", "Mustafa Abdullahi"),
            ("birthDate", "11.8.1960"),
            ("birthPlace", "Iran"),
            ("passportNumber", "D9004878"),
        ),
    ),
    ("A", "2"): (
        "AL-DIN Hasan Izz (a.k.a. Garbaya Ahmed, a.k.a. Sa’id, a.k.a. Salwwan"
        " Samir), Lebanon, born 1963 in Lebanon, citizen of Lebanon.",
        (
            ("name", "AL-DIN Hasan Izz"),
            ("alias", "Garbaya Ahmed"),
            ("alias", "Sa’id"),
            ("alias", "Salwwan Samir"),
            ("country", "Lebanon"),
            ("birthDate", "1963"),
            ("birthPlace", "Lebanon"),
            ("nationality", "Lebanon"),
        ),
    ),
    ("A", "3"): (
        "AL-NASSER Abdelkarim Hussein Mohamed, born in Al Ihsa (Saudi Arabia),"
        " citizen of Saudi Arabia.",
        (
            ("name", "AL-NASSER Abdelkarim Hussein Mohamed"),
            ("birthPlace", "Al Ihsa (Saudi Arabia)"),
            ("nationality", "Saudi Arabia"),
        ),
    ),
    ("A", "4"): (
        "AL-YACOUB Ibrahim Salih Mohammed, born 16.10.1966 in Tarut"
        " (Saudi Arabia), citizen of Saudi Arabia.",
        (
            ("name", "AL-YACOUB Ibrahim Salih Mohammed"),
            ("birthDate", "16.10.1966"),
            ("birthPlace", "Tarut (Saudi Arabia)"),
            ("nationality", "Saudi Arabia"),
        ),
    ),
    ("A", "5"): (
        "ARBABSIAR Manssor (a.k.a. Mansour Arbabsiar), born 6.3.1955 or"
        " 15.3.1955 in Iran. Iranian and US national, passport number:"
        " C2002515 (Iran); passport number: 477845448 (USA). National ID"
        " number: 07442833, expiry date 15.3.2016 (USA driving licence).",
        (
            ("name", "ARBABSIAR Manssor"),
            ("alias", "Mansour Arbabsiar"),
            ("birthDate", "6.3.1955 or 15.3.1955"),
            ("birthPlace", "Iran"),
            ("nationality", "Iranian and US"),
            ("passportNumber", "C2002515 (Iran)"),
            ("passportNumber", "477845448 (USA)"),
            ("idNumber", "07442833"),
        ),
    ),
    ("A", "6"): (
        "ASSADI Assadollah (a.k.a. Assadollah Asadi), born 22.12.1971 in"
        " Tehran (Iran), Iranian national. Iranian diplomatic passport"
        " number: D9016657.",
        (
            ("name", "ASSADI Assadollah"),
            ("alias", "Assadollah Asadi"),
            ("birthDate", "22.12.1971"),
            ("birthPlace", "Tehran (Iran)"),
            ("nationality", "Iranian"),
            ("passportNumber", "D9016657"),
        ),
    ),
    ("A", "7"): (
        "BOUYERI Mohammed (a.k.a. Abu Zubair, a.k.a. Sobiar, a.k.a. Abu"
        " Zoubair), born 8.3.1978 in Amsterdam (The Netherlands).",
        (
            ("name", "BOUYERI Mohammed"),
            ("alias", "Abu Zubair"),
            ("alias", "Sobiar"),
            ("alias", "Abu Zoubair"),
            ("birthDate", "8.3.1978"),
            ("birthPlace", "Amsterdam (The Netherlands)"),
        ),
    ),
    ("A", "8"): (
        "HASHEMI MOGHADAM Saeid, born 6.8.1962 in Tehran (Iran), Iranian"
        " national. Passport number: D9016290, valid until 4.2.2019.",
        (
            ("name", "HASHEMI MOGHADAM Saeid"),
            ("birthDate", "6.8.1962"),
            ("birthPlace", "Tehran (Iran)"),
            ("nationality", "Iranian"),
            ("passportNumber", "D9016290"),
        ),
    ),
    ("A", "9"): (
        "HASSAN EL HAJJ Hassan, born 22.3.1988 in Zaghdraiya, Sidon, Lebanon,"
        " Canadian citizen. Passport number: JX446643 (Canada).",
        (
            ("name", "HASSAN EL HAJJ Hassan"),
            ("birthDate", "22.3.1988"),
            ("birthPlace", "Zaghdraiya, Sidon, Lebanon"),
            ("nationality", "Canadian"),
            ("passportNumber", "JX446643 (Canada)"),
        ),
    ),
    ("A", "10"): (
        "MELIAD Farah, born 5.11.1980 in Sydney (Australia), Australian"
        " citizen. Passport number: M2719127 (Australia).",
        (
            ("name", "MELIAD Farah"),
            ("birthDate", "5.11.1980"),
            ("birthPlace", "Sydney (Australia)"),
            ("nationality", "Australian"),
            ("passportNumber", "M2719127 (Australia)"),
        ),
    ),
    ("A", "11"): (
        "MOHAMMED Khalid Sheikh (a.k.a. Ali Salem, a.k.a. Bin Khalid Fahd Bin"
        " Abdallah, a.k.a. Henin Ashraf Refaat Nabith, a.k.a. Wadood Khalid"
        " Abdul), born 14.4.1965 or 1.3.1964 in Pakistan, passport number"
        " 488555.",
        (
            ("name", "MOHAMMED Khalid Sheikh"),
            ("alias", "Ali Salem"),
            ("alias", "Bin Khalid Fahd Bin Abdallah"),
            ("alias", "Henin Ashraf Refaat Nabith"),
            ("alias", "Wadood Khalid Abdul"),
            ("birthDate", "14.4.1965 or 1.3.1964"),
            ("birthPlace", "Pakistan"),
            ("passportNumber", "488555"),
        ),
    ),
    ("A", "12"): (
        "SHAHLAI Abdul Reza (a.k.a. Abdol Reza Shala’i, a.k.a. Abd-al Reza"
        " Shalai, a.k.a. Abdorreza Shahlai, a.k.a. Abdolreza Shahla’i, a.k.a."
        " Abdul-Reza Shahlaee, a.k.a. Hajj Yusef, a.k.a. Haji Yusif, a.k.a."
        " Hajji Yasir, a.k.a. Hajji Yusif, a.k.a. Yusuf Abu-al-Karkh), born"
        " circa 1957 in Iran. Addresses: (1) Kermanshah, Iran, (2) Mehran"
        " Military Base, Ilam Province, Iran.",
        (
            ("name", "SHAHLAI Abdul Reza"),
            ("alias", "Abdol Reza Shala’i"),
            ("alias", "Abd-al Reza Shalai"),
            ("alias", "Abdorreza Shahlai"),
            ("alias", "Abdolreza Shahla’i"),
            ("alias", "Abdul-Reza Shahlaee"),
            ("alias", "Hajj Yusef"),
            ("alias", "Haji Yusif"),
            ("alias", "Hajji Yasir"),
            ("alias", "Hajji Yusif"),
            ("alias", "Yusuf Abu-al-Karkh"),
            ("birthDate", "circa 1957"),
            ("birthPlace", "Iran"),
            ("address", "Kermanshah, Iran"),
            ("address", "Mehran Military Base, Ilam Province, Iran"),
        ),
    ),
    ("A", "13"): (
        "SHAKURI Ali Gholam, born circa 1965 in Tehran, Iran.",
        (
            ("name", "SHAKURI Ali Gholam"),
            ("birthDate", "circa 1965"),
            ("birthPlace", "Tehran, Iran"),
        ),
    ),
    ("B", "1"): (
        "‘Abu Nidal Organisation’ – ‘ANO’ (a.k.a. ‘Fatah Revolutionary"
        " Council’, a.k.a. ‘Arab Revolutionary Brigades’, a.k.a. ‘Black"
        " September’, a.k.a. ‘Revolutionary Organisation of Socialist"
        " Muslims’).",
        (
            ("name", "Abu Nidal Organisation"),
            ("alias", "ANO"),
            ("alias", "Fatah Revolutionary Council"),
            ("alias", "Arab Revolutionary Brigades"),
            ("alias", "Black September"),
            ("alias", "Revolutionary Organisation of Socialist Muslims"),
        ),
    ),
    ("B", "2"): (
        "‘Al-Aqsa Martyrs’ Brigade’.",
        (("name", "Al-Aqsa Martyrs’ Brigade"),),
    ),
    ("B", "3"): (
        "‘Al-Aqsa e.V.’.",
        (("name", "Al-Aqsa e.V."),),
    ),
    ("B", "4"): (
        "‘Babbar Khalsa’.",
        (("name", "Babbar Khalsa"),),
    ),
    ("B", "5"): (
        "‘Communist Party of the Philippines’, including ‘New People’s Army’"
        " – ‘NPA’, Philippines.",
        (
            ("name", "Communist Party of the Philippines"),
            (
                "notes",
                "including ‘New People’s Army’ – ‘NPA’, Philippines",
            ),
        ),
    ),
    ("B", "6"): (
        "Directorate for Internal Security of the Iranian Ministry for"
        " Intelligence and Security.",
        (
            (
                "name",
                "Directorate for Internal Security of the Iranian Ministry"
                " for Intelligence and Security",
            ),
        ),
    ),
    ("B", "7"): (
        "‘Gama’a al-Islamiyya’ (a.k.a. ‘Al-Gama’a al-Islamiyya’) (‘Islamic"
        " Group’ – ‘IG’).",
        (
            ("name", "Gama’a al-Islamiyya"),
            ("alias", "Al-Gama’a al-Islamiyya"),
            ("alias", "Islamic Group"),
            ("alias", "IG"),
        ),
    ),
    ("B", "8"): (
        "‘İslami Büyük Doğu Akıncılar Cephesi’ – ‘IBDA-C’ (‘Great Islamic"
        " Eastern Warriors Front’).",
        (
            ("name", "İslami Büyük Doğu Akıncılar Cephesi"),
            ("alias", "IBDA-C"),
            ("alias", "Great Islamic Eastern Warriors Front"),
        ),
    ),
    ("B", "9"): (
        "‘Islamic Revolutionary Guard Corps (IRGC)’.",
        (("name", "Islamic Revolutionary Guard Corps (IRGC)"),),
    ),
    ("B", "10"): (
        "‘Hamas’, including ‘Hamas-Izz al-Din al-Qassem’.",
        (
            ("name", "Hamas"),
            ("notes", "including ‘Hamas-Izz al-Din al-Qassem’"),
        ),
    ),
    ("B", "11"): (
        "‘Hizballah Military Wing’ (a.k.a. ‘Hezbollah Military Wing’, a.k.a."
        " ‘Hizbullah Military Wing’, a.k.a. ‘Hizbollah Military Wing’, a.k.a."
        " ‘Hezballah Military Wing’, a.k.a. ‘Hisbollah Military Wing’, a.k.a."
        " ‘Hizbu’llah Military Wing’ a.k.a. ‘Hizb Allah Military Wing’,"
        " a.k.a. ‘Jihad Council’ (and all units reporting to it, including"
        " the External Security Organisation)).",
        (
            ("name", "Hizballah Military Wing"),
            ("alias", "Hezbollah Military Wing"),
            ("alias", "Hizbullah Military Wing"),
            ("alias", "Hizbollah Military Wing"),
            ("alias", "Hezballah Military Wing"),
            ("alias", "Hisbollah Military Wing"),
            ("alias", "Hizbu’llah Military Wing"),
            ("alias", "Hizb Allah Military Wing"),
            ("alias", "Jihad Council"),
            (
                "notes",
                "and all units reporting to it, including the External"
                " Security Organisation",
            ),
        ),
    ),
    ("B", "12"): (
        "‘Hizbul Mujahideen’ – ‘HM’.",
        (("name", "Hizbul Mujahideen"), ("alias", "HM")),
    ),
    ("B", "13"): (
        "‘Khalistan Zindabad Force’ – ‘KZF’.",
        (("name", "Khalistan Zindabad Force"), ("alias", "KZF")),
    ),
    ("B", "14"): (
        "‘Kurdistan Workers’ Party’ – ‘PKK’ (a.k.a. ‘KADEK’, a.k.a. ‘KONGRA-GEL’).",
        (
            ("name", "Kurdistan Workers’ Party"),
            ("alias", "PKK"),
            ("alias", "KADEK"),
            ("alias", "KONGRA-GEL"),
        ),
    ),
    ("B", "15"): (
        "‘Liberation Tigers of Tamil Eelam’ – ‘LTTE’.",
        (("name", "Liberation Tigers of Tamil Eelam"), ("alias", "LTTE")),
    ),
    ("B", "16"): (
        "‘Ejército de Liberación Nacional’ (‘National Liberation Army’).",
        (
            ("name", "Ejército de Liberación Nacional"),
            ("alias", "National Liberation Army"),
        ),
    ),
    ("B", "17"): (
        "‘Palestinian Islamic Jihad’ – ‘PIJ’.",
        (("name", "Palestinian Islamic Jihad"), ("alias", "PIJ")),
    ),
    ("B", "18"): (
        "‘Popular Front for the Liberation of Palestine’ – ‘PFLP’.",
        (
            ("name", "Popular Front for the Liberation of Palestine"),
            ("alias", "PFLP"),
        ),
    ),
    ("B", "19"): (
        "‘Popular Front for the Liberation of Palestine – General Command’"
        " (a.k.a. ‘PFLP – General Command’).",
        (
            (
                "name",
                "Popular Front for the Liberation of Palestine – General Command",
            ),
            ("alias", "PFLP – General Command"),
        ),
    ),
    ("B", "20"): (
        "‘Devrimci Halk Kurtuluș Partisi-Cephesi’ – ‘DHKP/C’ (a.k.a."
        " ‘Devrimci Sol’ (‘Revolutionary Left’), a.k.a. ‘Dev Sol’)"
        " (‘Revolutionary People’s Liberation Army/Front/Party’).",
        (
            ("name", "Devrimci Halk Kurtuluș Partisi-Cephesi"),
            ("alias", "DHKP/C"),
            ("alias", "Devrimci Sol"),
            ("alias", "Revolutionary Left"),
            ("alias", "Dev Sol"),
            ("alias", "Revolutionary People’s Liberation Army/Front/Party"),
        ),
    ),
    ("B", "21"): (
        "‘Sendero Luminoso’ – ‘SL’ (‘Shining Path’).",
        (
            ("name", "Sendero Luminoso"),
            ("alias", "SL"),
            ("alias", "Shining Path"),
        ),
    ),
    ("B", "22"): (
        "‘Teyrbazen Azadiya Kurdistan’ – ‘TAK’ (a.k.a. ‘Kurdistan Freedom"
        " Falcons’, a.k.a. ‘Kurdistan Freedom Hawks’).",
        (
            ("name", "Teyrbazen Azadiya Kurdistan"),
            ("alias", "TAK"),
            ("alias", "Kurdistan Freedom Falcons"),
            ("alias", "Kurdistan Freedom Hawks"),
        ),
    ),
    ("B", "23"): (
        "‘The Base’.",
        (("name", "The Base"),),
    ),
}


def check_registry() -> None:
    program = get_program_by_key(PROGRAM_KEY)
    if program is None:
        raise ParseError(f"unknown program key {PROGRAM_KEY!r}")
    if MEASURE not in get_args(Measure):
        raise ParseError(f"invalid measure {MEASURE!r}")
    if MEASURE not in program.measures:
        raise ParseError(f"measure {MEASURE!r} not in {PROGRAM_KEY}")
    for _, _, schema_name in PARTS:
        if model.get(schema_name) is None:
            raise ParseError(f"unknown schema {schema_name!r}")


def build_row(annex: str, part: str, schema: str, text: str) -> Row:
    number = NUMBER_RE.match(text)
    if number is None:
        raise ParseError(f"{annex}.{part}: unnumbered entry {text[:60]!r}")
    record_id, sentence = number.group(1), number.group(2)
    ctx = f"{annex}.{part} entry {record_id}"
    reviewed = ENTRIES.get((part, record_id))
    if reviewed is None:
        raise ParseError(f"{ctx}: entry has no reviewed transcription")
    reviewed_text, mapping = reviewed
    if sentence != reviewed_text:
        raise ParseError(f"{ctx}: printed text differs from reviewed transcription")
    row = Row(annex_id(annex, part), schema, MEASURE, record_id=record_id)
    for column, value in mapping:
        row.add(column, [value])
    return row


def part_walker(annex: str, block: Element) -> list[tuple[str, str, list[Element]]]:
    """Group the annex's entry elements under the expected part headings."""
    parts: list[tuple[str, str, list[Element]]] = []
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            if cls == "" and clean(element_text(child), annex) != "":
                raise ParseError(f"{annex}: unexpected bare paragraph text")
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-1":
            heading = clean(element_text(child), annex)
            index = len(parts)
            if index >= len(PARTS) or PARTS[index][0] != heading:
                raise ParseError(f"{annex}: unexpected part heading {heading!r}")
            parts.append((PARTS[index][1], PARTS[index][2], []))
            continue
        if not parts:
            raise ParseError(f"{annex}: content before first part heading")
        parts[-1][2].append(child)
    if len(parts) != len(PARTS):
        raise ParseError(f"{annex}: expected {len(PARTS)} parts, got {len(parts)}")
    return parts


def parse_annex_ii(annex: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    for part, schema, elements in part_walker(annex, block):
        count = 0
        for element in elements:
            if element.tag != "div":
                raise ParseError(f"{annex}.{part}: unexpected <{element.tag}>")
            ctx = f"{annex}.{part}"
            text = single_paragraph(element, ctx)
            row = build_row(annex, part, schema, text)
            count += 1
            if row.record_id != str(count):
                raise ParseError(f"{ctx}: entry numbering gap at {row.record_id}")
            rows.append(row)
        reviewed = sum(1 for key in ENTRIES if key[0] == part)
        if count != reviewed:
            raise ParseError(
                f"{annex}.{part}: {count} entries printed, {reviewed} reviewed"
            )
    return rows


def check_placeholder_annex(annex: str, block: Element) -> None:
    for part, _, elements in part_walker(annex, block):
        texts = [clean(element_text(el), f"{annex}.{part}") for el in elements]
        if texts != [PLACEHOLDER]:
            raise ParseError(
                f"{annex}.{part}: expected only the {PLACEHOLDER!r} placeholder"
            )


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    known = {TARGET_ANNEX, PLACEHOLDER_ANNEX} | NON_TARGET
    for annex, block in annex_blocks(doc, known):
        if annex in NON_TARGET:
            continue
        if annex == PLACEHOLDER_ANNEX:
            check_placeholder_annex(annex, block)
            continue
        annex_rows = parse_annex_ii(annex, block)
        if not annex_rows:
            raise ParseError(f"{annex}: no entries extracted")
        rows.extend(annex_rows)
    return rows


@click.command(help="Parse consolidated Regulation 2580/2001 into a CSV candidate.")
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
            raise ParseError(f"not a consolidated 2580/2001 CELEX: {celex!r}")
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
