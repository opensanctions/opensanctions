"""The op-log CSV contract shared by the tools in this directory.

Each file under ``data/regulations/{framework}/`` transcribes one legal act as
a sequence of ``ADD``/``MOD``/``DEL`` ops on the framework's designations.
This module holds what every tool must agree on: the column set, the op
vocabulary, the multi-value cell codec, file access, and how a designation's
``id`` is minted. Nothing here imports from ``../scripts/``.
"""

import csv
import io
import re
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path

from followthemoney.util import make_entity_id
from zavod.shed.ojeu.cellar import RelatedAct, cli_client

TOOLS_DIR = Path(__file__).resolve().parent
DATASET_DIR = TOOLS_DIR.parent
REGULATIONS_DIR = DATASET_DIR / "data" / "regulations"

# Hash namespace of `Context.make_id` for this dataset; a minted `id` becomes
# today's entity ID once the crawler adds the dataset prefix.
ID_KEY_PREFIX = "eu_journal_sanctions"

OPS = ("ADD", "MOD", "DEL")

OP_COLUMNS = (
    "op",
    "id",
    "publishedDate",
    "effectiveDate",
)
METADATA_COLUMNS = (
    "recordId",
    "programKey",
    "annex",
    "measure",
    "listingDate",
    "endDate",
    "schema",
)
ENTITY_COLUMNS = (
    "name",
    "alias",
    "weakAlias",
    "previousName",
    "country",
    "nationality",
    "jurisdiction",
    "birthDate",
    "birthPlace",
    "position",
    "passportNumber",
    "gender",
    "appearance",
    "ethnicity",
    "fatherName",
    "motherName",
    "incorporationDate",
    "legalForm",
    "sector",
    "registrationNumber",
    "taxNumber",
    "idNumber",
    "innCode",
    "ogrnCode",
    "kppCode",
    "okpoCode",
    "leiCode",
    "swiftBic",
    "ticker",
    "imoNumber",
    "mmsi",
    "flag",
    "address",
    "phone",
    "email",
    "website",
    "notes",
)
# `extra` keeps printed `Label: value` pairs that no column fits.
OPLOG_COLUMNS = OP_COLUMNS + METADATA_COLUMNS + ENTITY_COLUMNS + ("extra", "reason")
# Columns holding `;`-separated values; every other column is scalar.
MULTI_COLUMNS = tuple(c for c in ENTITY_COLUMNS if c != "name") + ("extra", "reason")

# The only cells a DEL row may fill: enough to target the entry and to
# cross-check it against the live state.
DEL_COLUMNS = (
    "op",
    "id",
    "publishedDate",
    "effectiveDate",
    "recordId",
    "programKey",
    "annex",
    "measure",
    "schema",
    "name",
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


def loosen(text: str) -> str:
    """Casefold and unify quotes and dashes, for consolidation lookups."""
    return text.casefold().translate(LOOSE_CHARS)


def haystack(text: str, loose: bool = False) -> str:
    """Flatten a rendered `text.txt` for value lookups.

    Render separators and consolidation change markers become spaces, and
    whitespace collapses, so a value split across paragraphs or cells by the
    rendering still matches. `loose` also applies `loosen`.
    """
    text = SEPARATOR_RE.sub(" ", MARKER_RE.sub(" ", text))
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


MONTHS = {
    m: i
    for i, m in enumerate(
        ("january", "february", "march", "april", "may", "june", "july")
        + ("august", "september", "october", "november", "december"),
        start=1,
    )
}
DOTTED_RE = re.compile(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$")
# Full month names ("2 December 1985") and UN abbreviations ("26 Feb. 2011").
WORDED_RE = re.compile(r"^(\d{1,2}) ([A-Za-z]+)(\.?) (\d{4})$")


def parse_date(text: str) -> tuple[int, int, int] | None:
    """Read a full printed date; partial or other shapes give None."""
    dotted = DOTTED_RE.match(text.strip())
    if dotted is not None:
        day, month, year = dotted.groups()
        return (int(year), int(month), int(day))
    worded = WORDED_RE.match(text.strip())
    if worded is None:
        return None
    day, word, dot, year = worded.groups()
    word = word.lower()
    if dot:
        found = [n for m, n in MONTHS.items() if m.startswith(word) and len(word) >= 3]
        month = found[0] if len(found) == 1 else None
    else:
        month = MONTHS.get(word)
    return None if month is None else (int(year), month, int(day))


def mint_id(framework: str, record_id: str, name: str) -> str:
    """Mint the `id` for a designation at its first ADD.

    Uses the formula the crawler has always applied to (framework, entry
    number, name), with the name as first listed. Once written, the `id` is
    carried verbatim by every later op, so renames never change it.
    """
    hashed = make_entity_id(framework, record_id, name, key_prefix=ID_KEY_PREFIX)
    if hashed is None:
        raise ValueError("cannot mint an id from empty parts")
    return hashed


def join_multi(values: list[str]) -> str:
    """Encode values into one multi-value cell, losslessly.

    Values are "; "-joined; a value that contains the separator or a quote
    is CSV-quoted (embedded quotes doubled) so the cell decodes with a
    ";"-delimiter CSV parser.
    """
    encoded: list[str] = []
    for value in values:
        if ";" in value or '"' in value:
            value = '"' + value.replace('"', '""') + '"'
        encoded.append(value)
    return "; ".join(encoded)


def split_multi(value: str) -> list[str]:
    """Decode a multi-value cell into trimmed elements, inverting `join_multi`.

    Raises:
        ValueError: The cell does not decode to one clean row.
    """
    if value == "":
        return []
    try:
        rows = list(
            csv.reader(io.StringIO(value), delimiter=";", skipinitialspace=True)
        )
    except csv.Error as exc:
        raise ValueError(f"multi-value cell does not decode as ;-CSV: {exc}")
    if len(rows) != 1:
        raise ValueError("multi-value cell contains a line break")
    return [element.strip() for element in rows[0]]


def cell_values(row: dict[str, str], column: str) -> list[str]:
    """Read a cell as a list: decoded if multi-valued, else empty or one value."""
    if column in MULTI_COLUMNS:
        return split_multi(row[column])
    return [row[column]] if row[column] != "" else []


def encode_row(values: Mapping[str, str | Sequence[str]]) -> dict[str, str]:
    """Build a complete op-log row from values keyed by column.

    Use this when writing op rows from a script: multi-valued columns take a
    list (or a single string) and are encoded with `join_multi`; scalar
    columns take a string; omitted columns are empty.

    Raises:
        ValueError: An unknown column, a list for a scalar column, or an
            empty or repeated element.
    """
    row = dict.fromkeys(OPLOG_COLUMNS, "")
    for column, value in values.items():
        if column not in row:
            raise ValueError(f"not an op-log column: {column!r}")
        if isinstance(value, str):
            row[column] = value.strip()
            continue
        if column not in MULTI_COLUMNS:
            raise ValueError(f"{column} is scalar, got {value!r}")
        elements = [element.strip() for element in value]
        if "" in elements or len(set(elements)) != len(elements):
            raise ValueError(f"{column} has empty or repeated values: {value!r}")
        row[column] = join_multi(elements)
    return row


def regime_files(framework: str) -> list[Path]:
    """List the op-log files of one framework act, in filename order."""
    directory = REGULATIONS_DIR / framework
    if not directory.is_dir():
        raise ValueError(f"no op-log directory for {framework}: {directory}")
    return sorted(directory.glob("*.csv"))


def read_oplog(path: Path) -> list[dict[str, str]]:
    """Read one op-log file, requiring the exact contract header.

    Raises:
        ValueError: The header differs from `OPLOG_COLUMNS` or a row has the
            wrong number of cells.
    """
    with open(path, encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        header = tuple(next(reader, ()))
        if header != OPLOG_COLUMNS:
            raise ValueError(
                f"{path}: header does not match the op-log contract;"
                " `python -m tools.migrate` brings it up to date"
            )
        rows: list[dict[str, str]] = []
        for line, cells in enumerate(reader, start=2):
            if len(cells) != len(header):
                raise ValueError(f"{path}:{line}: expected {len(header)} cells")
            rows.append(dict(zip(header, cells)))
    return rows


def write_oplog(path: Path, rows: list[dict[str, str]]) -> None:
    """Write one op-log file with the contract header."""
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, OPLOG_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def related_acts(framework: str) -> list[RelatedAct]:
    """List the acts CELLAR links to a framework, one per CELEX, by date.

    Both relations count: implementing regulations often link to their
    framework only as their legal basis (`based_on`), not as `amends`.
    Corrigenda of the framework and of those acts are included, with
    relation "corrects".
    """
    with cli_client() as client:
        found = list(client.query_related_acts(framework, date(1990, 1, 1)))
        targets = {framework} | {act.celex for act in found}
        found.extend(client.query_corrigenda(sorted(targets)))
    unique = {act.celex: act for act in found}
    return sorted(unique.values(), key=lambda act: (act.document_date, act.celex))
