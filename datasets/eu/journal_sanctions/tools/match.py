"""Match names and values: earlier designations, and printed text.

Amending acts address existing entries by name, often in a different form
than the one listed: reordered ("Ozerov, Viktor" / "Viktor OZEROV"), with a
patronymic added, a title prefixed, or a different transliteration. `rank`
compares transliterated token sets fuzzily across every name-like value on
both sides, and weighs in the printed listing date (replacement entries
reprint it) and the date of birth. `missing_values` looks a row's values up
in an act's or a consolidation's text.
"""

import re
from dataclasses import dataclass

from normality import ascii_text
from rigour.names import tokenize_name
from rigour.text.distance import levenshtein_similarity

from .common import OP_COLUMNS, OPLOG_COLUMNS, cell_values, parse_date, split_multi
from .replay import Entry

NAME_COLUMNS = ("name", "alias", "weakAlias", "previousName")
PAREN_RE = re.compile(r"\(([^()]*)\)")
AKA_RE = re.compile(r"\b(?:a\.k\.a\.?|f\.k\.a\.?|alias)\s", re.IGNORECASE)
# Quotes often abut words without a space (‘Crimea’‘Chernomorneftegaz’).
QUOTES_RE = re.compile(r"[‘’“”'\"«»]")
TOKEN_MATCH = 0.7
LISTING_BONUS = 0.3
RECORD_BONUS = 0.15
ANNEX_BONUS = 0.1
PRIMARY_BONUS = 0.2
# A match through an alias ranks below the same match on the primary names:
# UN-style lists reuse one person's alias as another's name.
ALIAS_WEIGHT = 0.9
BIRTH_BONUS = 0.2
BIRTH_CONFLICT = -0.2
ACCEPT = 0.9
CONTAINED = 0.7
MARGIN = 0.25


def name_forms(value: str) -> list[list[str]]:
    """Token lists for a printed name and each parenthesised or a.k.a. part."""
    parts = [PAREN_RE.sub(" ", value), *PAREN_RE.findall(value)]
    forms: list[list[str]] = []
    for part in parts:
        for segment in AKA_RE.split(part):
            text = ascii_text(QUOTES_RE.sub(" ", segment)) or ""
            tokens = [t for t in tokenize_name(text.casefold()) if len(t) > 1]
            if tokens:
                forms.append(tokens)
    return forms


def row_forms(row: dict[str, str]) -> list[list[str]]:
    forms: list[list[str]] = []
    for column in NAME_COLUMNS:
        for value in cell_values(row, column):
            forms.extend(name_forms(value))
    return forms


def token_overlap(left: list[str], right: list[str]) -> float:
    """Dice-style overlap of two token lists, with fuzzy token matches.

    A name extended by more parts ("Taha" / "Ahmad Taha Khalid Abdul Qadir")
    keeps a floor score when every token of the shorter one is matched.
    """
    unused = list(right)
    matched, count = 0.0, 0
    for token in left:
        best, best_sim = None, 0.0
        for other in unused:
            sim = levenshtein_similarity(token, other)
            if sim > best_sim:
                best, best_sim = other, sim
        if best is not None and best_sim >= TOKEN_MATCH:
            unused.remove(best)
            matched += best_sim
            count += 1
    dice = 2 * matched / (len(left) + len(right))
    if count == min(len(left), len(right)):
        return max(dice, CONTAINED)
    return dice


def name_score(query: list[list[str]], candidate: list[list[str]]) -> float:
    return max((token_overlap(q, c) for q in query for c in candidate), default=0.0)


def dates(cell: str) -> set[tuple[int, int, int]]:
    found = {parse_date(value) for value in split_multi(cell) if value}
    return {d for d in found if d is not None}


@dataclass
class Match:
    entry: Entry
    score: float
    evidence: str


def rank(row: dict[str, str], entries: list[Entry]) -> list[Match]:
    """Score live entries of the same schema, program and measure against a row."""
    query = row_forms(row)
    primary = name_forms(row["name"])
    births = dates(row["birthDate"])
    listing = parse_date(row["listingDate"])
    matches: list[Match] = []
    for entry in entries:
        other = entry.row
        if (other["schema"], other["programKey"], other["measure"]) != (
            row["schema"],
            row["programKey"],
            row["measure"],
        ):
            continue
        score = max(
            name_score(primary, name_forms(other["name"])),
            ALIAS_WEIGHT * name_score(query, row_forms(other)),
        )
        evidence = [f"name {score:.2f}"]
        # The act names the entry exactly as it is listed.
        if any(
            sorted(q) == sorted(c) for q in primary for c in name_forms(other["name"])
        ):
            score += PRIMARY_BONUS
            evidence.append("primary")
        if row["annex"] != "" and row["annex"] == other["annex"]:
            score += ANNEX_BONUS
            evidence.append("annex")
        if row["recordId"] != "" and row["recordId"] == other["recordId"]:
            score += RECORD_BONUS
            evidence.append("recordId")
        if listing is not None and listing == parse_date(other["listingDate"]):
            score += LISTING_BONUS
            evidence.append("listingDate")
        other_births = dates(other["birthDate"])
        if births and other_births:
            if births & other_births:
                score += BIRTH_BONUS
                evidence.append("birthDate")
            else:
                score += BIRTH_CONFLICT
                evidence.append("birthDate conflict")
        matches.append(Match(entry, score, ", ".join(evidence)))
    matches.sort(key=lambda m: m.score, reverse=True)
    return matches


def decide(matches: list[Match]) -> Match | None:
    """The top match if it is strong and clearly ahead of the runner-up."""
    if not matches or matches[0].score < ACCEPT:
        return None
    if len(matches) > 1 and matches[0].score - matches[1].score < MARGIN:
        return None
    return matches[0]


def describe(match: Match) -> str:
    row = match.entry.row
    return (
        f"{match.score:.2f} {row['id'][:10]} {row['annex']}#{row['recordId']} "
        f"{row['name']} [{match.evidence}]"
    )


# Columns that carry the transcriber's context rather than printed text, so
# they are never looked up in an act's text.
CONTEXT_COLUMNS = frozenset(
    OP_COLUMNS + ("programKey", "measure", "schema", "annex", "recordId")
)
# EUR-Lex consolidation change markers: "▼B", "▼M12", "►C3", "◄".
MARKER_RE = re.compile(r"[▼►][A-Z]\d*(?: —+)?|◄")
SEPARATOR_RE = re.compile(r" [¶|‖] ")


# Typographic variants EUR-Lex editors change between an act and its
# consolidation.
LOOSE_CHARS = str.maketrans(
    {c: "'" for c in '‘’“”„«»"`´'}
    | {c: "-" for c in "‐‑‒–—"}
    # Cyrillic letters that look like Latin ones and get swapped in print.
    | dict(zip("аеіорсхуј", "aeiopcxyj"))
)


# UN lists print names in numbered parts ("1: ABU 2: ZAYD"); transcriptions
# join them.
NAME_PART_RE = re.compile(r"(?<![\w:])\d{1,2}: ")


def loosen(text: str) -> str:
    """Casefold and unify quotes, dashes and lookalike letters, for
    consolidation lookups."""
    return text.casefold().translate(LOOSE_CHARS)


def haystack(text: str, loose: bool = False) -> str:
    """Flatten a rendered `text.txt` for value lookups.

    Render separators and consolidation change markers become spaces, and
    whitespace collapses, so a value split across paragraphs or cells by the
    rendering still matches, and UN name-part labels go, as transcriptions
    join the parts. `loose` also applies `loosen`.
    """
    text = SEPARATOR_RE.sub(" ", MARKER_RE.sub(" ", text))
    text = NAME_PART_RE.sub("", text)
    text = " ".join(text.split())
    return loosen(text) if loose else text


def missing_values(
    row: dict[str, str],
    text: str,
    loose: bool = False,
    carried: dict[str, str] | None = None,
) -> list[tuple[str, str]]:
    """List the (column, value) pairs of a row not printed in `text`.

    `text` is a `haystack` built with the same `loose`. Multi-value cells are
    looked up element by element; context columns are skipped. A MOD or DEL
    reproduces its target's row, so values equal to the target's (`carried`)
    need not be printed in the act.
    """
    missing: list[tuple[str, str]] = []
    for column in OPLOG_COLUMNS:
        if column in CONTEXT_COLUMNS or row[column] == "":
            continue
        previous = cell_values(carried, column) if carried is not None else []
        for value in cell_values(row, column):
            if value in previous:
                continue
            needle = " ".join(value.split())
            if (loosen(needle) if loose else needle) not in text:
                missing.append((column, value))
    return missing
