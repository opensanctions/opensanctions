"""Validate reviewed EU Journal sanctions CSV files against the column contract.

Reviewed designations live in git as amendment and consolidated CSV files with
a fixed column contract, documented in ``data/FORMAT.md`` next to the files.
This tool checks those files offline — structure, CELEX provenance, program and
measure vocabulary, FtM schema compatibility, dates, multi-value cells, and row
uniqueness — so contract violations are caught while a reviewer still has the
source act open, not once the data is published.

It checks structure only: it cannot verify that a cell matches the source act.
"""

import csv
import io
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, get_args

import click
from followthemoney import model
from rigour.dates import prefix_interval
from zavod.shed.ojeu.celex import normalize as normalize_celex
from zavod.stateful.programs import Measure, get_program_by_key

from common import (
    AMENDMENT_COLUMNS,
    CONSOLIDATED_COLUMNS,
    DATASET_DIR,
    ENTITY_COLUMNS,
    parse_abbrev_date,
    parse_dotted_date,
    parse_worded_date,
    split_multi,
)

FileKind = Literal["amendment", "consolidated"]

# Every entity property column except name holds `;`-separated multi-values.
# All date columns hold the source's printed wording, normalized in the
# crawler via the dataset `dates` configuration and `type.date` lookups. The
# entity date columns (birthDate, incorporationDate) are free-form; startDate
# is shape-checked only to guard that it is one bare date.
MULTI_VALUE_COLUMNS: frozenset[str] = frozenset(ENTITY_COLUMNS) - {"name"}
SUPPORTED_SCHEMAS: frozenset[str] = frozenset(
    {"Person", "LegalEntity", "Organization", "Company", "Vessel", "Asset"}
)
MEASURES: frozenset[str] = frozenset(get_args(Measure))
PLACEHOLDER_VALUES: frozenset[str] = frozenset(
    {"unknown", "n/a", "-", "--", "none", "?"}
)


@dataclass(frozen=True)
class Issue:
    """One contract violation, located as precisely as the check allows."""

    file: Path
    row: int | None
    column: str | None
    message: str

    def __str__(self) -> str:
        location = str(self.file)
        if self.row is not None:
            location = f"{location}:{self.row}"
        if self.column is not None:
            location = f"{location}:{self.column}"
        return f"{location}: {self.message}"

    def sort_key(self) -> tuple[str, int, str]:
        return (str(self.file), self.row or 0, self.column or "")


@dataclass(frozen=True)
class ValidationResult:
    """Parsed rows and every issue found in one reviewed CSV file."""

    kind: FileKind | None
    rows: list[dict[str, str]]
    issues: list[Issue]


def _check_celex(value: str) -> str | None:
    """Return an error message if the value is not one bare normalized CELEX."""
    if ";" in value:
        return "cell contains multiple CELEX values"
    try:
        normalized = normalize_celex(value)
    except ValueError:
        return f"invalid CELEX identifier: {value!r}"
    if normalized != value:
        return f"CELEX is not in normalized form: {value!r} (expected {normalized!r})"
    return None


# The ISO-partial shape accepted in startDate; the dotted, worded and
# UN-abbreviated shapes are recognized by the shared parsers in common.py.
DATE_ISO_RE = re.compile(r"^(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?$")


def _check_start_date(value: str) -> str | None:
    """Return an error message unless the value is one bare, calendar-valid date."""
    iso: str | None = None
    if DATE_ISO_RE.match(value) is not None:
        iso = value
    else:
        iso = (
            parse_dotted_date(value)
            or parse_worded_date(value)
            or parse_abbrev_date(value)
        )
    if iso is None:
        return (
            "startDate must be one bare date (ISO partial, dotted, worded, "
            f"or UN-abbreviated): {value!r}"
        )
    try:
        prefix_interval(iso)
    except ValueError:
        return f"invalid calendar date: {value!r}"
    return None


def _read_rows(path: Path, issues: list[Issue]) -> list[list[str]] | None:
    try:
        text = path.read_text(encoding="utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        issues.append(Issue(path, None, None, f"file is not valid UTF-8: {exc}"))
        return None
    reader = csv.reader(io.StringIO(text), strict=True)
    try:
        return list(reader)
    except csv.Error as exc:
        issues.append(Issue(path, reader.line_num, None, f"malformed CSV: {exc}"))
        return None


def validate_file(path: Path) -> ValidationResult:
    """Validate one reviewed CSV file and return its rows and issues.

    Contract violations become ``Issue`` entries rather than exceptions, so a
    single run reports everything wrong with a file. Rows are returned as raw
    trimmed-cell mappings only for callers that have checked ``issues`` is
    empty.
    """
    issues: list[Issue] = []
    raw_rows = _read_rows(path, issues)
    if raw_rows is None:
        return ValidationResult(None, [], issues)
    if len(raw_rows) == 0:
        issues.append(Issue(path, None, None, "file is empty"))
        return ValidationResult(None, [], issues)

    header = tuple(raw_rows[0])
    if header == AMENDMENT_COLUMNS:
        kind: FileKind = "amendment"
    elif header == CONSOLIDATED_COLUMNS:
        kind = "consolidated"
    else:
        issues.append(
            Issue(path, 1, None, "header matches neither CSV file kind exactly")
        )
        return ValidationResult(None, [], issues)
    if len(raw_rows) == 1:
        # An amendment transcribes at least one change; a consolidated file
        # may legitimately snapshot a framework act with empty annexes.
        if kind == "amendment":
            issues.append(Issue(path, None, None, "file contains no data rows"))
        return ValidationResult(kind, [], issues)

    source_column = "amendmentCelex" if kind == "amendment" else "celex"
    framework_column = "amendedCelex" if kind == "amendment" else "celex"
    source_values: set[str] = set()
    seen_rows: dict[tuple[str, ...], int] = {}
    seen_record_ids: dict[tuple[str, str, str, str, str], dict[str, int]] = {}
    records: list[dict[str, str]] = []

    for row_num, cells in enumerate(raw_rows[1:], start=2):
        if len(cells) != len(header):
            issues.append(
                Issue(
                    path,
                    row_num,
                    None,
                    f"row has {len(cells)} columns, expected {len(header)}",
                )
            )
            continue
        record = dict(zip(header, cells, strict=True))
        records.append(record)

        for column, value in record.items():
            if value != value.strip():
                issues.append(
                    Issue(
                        path, row_num, column, "cell has leading or trailing whitespace"
                    )
                )
            if value.strip().lower() in PLACEHOLDER_VALUES:
                issues.append(
                    Issue(
                        path,
                        row_num,
                        column,
                        f"placeholder value {value!r}; leave the cell empty instead",
                    )
                )

        # CELEX provenance columns.
        celex_columns = (
            ("amendedCelex", "amendmentCelex") if kind == "amendment" else ("celex",)
        )
        for column in celex_columns:
            value = record[column]
            if value == "":
                issues.append(Issue(path, row_num, column, "CELEX value is missing"))
                continue
            error = _check_celex(value)
            if error is not None:
                issues.append(Issue(path, row_num, column, error))
                continue
            if "-" in value:
                issues.append(
                    Issue(
                        path,
                        row_num,
                        column,
                        f"CELEX must not carry a date suffix: {value!r}",
                    )
                )
            if column == source_column:
                source_values.add(value)

        # Required values.
        for column in ("schema", "name", "programKey", "measure"):
            if record[column] == "":
                issues.append(Issue(path, row_num, column, "required value is missing"))

        # Schema and property compatibility.
        schema_name = record["schema"]
        if schema_name != "" and schema_name not in SUPPORTED_SCHEMAS:
            issues.append(
                Issue(path, row_num, "schema", f"unsupported schema: {schema_name!r}")
            )
        elif schema_name != "":
            schema = model.get(schema_name)
            if schema is None:
                raise RuntimeError(f"Contract schema missing from FtM: {schema_name}")
            for column in ENTITY_COLUMNS:
                if record[column] != "" and schema.get(column) is None:
                    issues.append(
                        Issue(
                            path,
                            row_num,
                            column,
                            f"property does not exist on schema {schema_name}",
                        )
                    )

        # Program and measure vocabulary.
        program_key = record["programKey"]
        program = None
        if program_key != "":
            program = get_program_by_key(program_key)
            if program is None:
                issues.append(
                    Issue(
                        path, row_num, "programKey", f"unknown program: {program_key!r}"
                    )
                )
        measure = record["measure"]
        if measure != "":
            if measure not in MEASURES:
                issues.append(
                    Issue(path, row_num, "measure", f"not a valid Measure: {measure!r}")
                )
            elif program is not None and measure not in program.measures:
                issues.append(
                    Issue(
                        path,
                        row_num,
                        "measure",
                        f"program {program_key} does not declare measure {measure!r}",
                    )
                )

        # Dates: only the scalar startDate is shape-checked.
        if record["startDate"] != "":
            error = _check_start_date(record["startDate"])
            if error is not None:
                issues.append(Issue(path, row_num, "startDate", error))

        # Multi-value cells.
        for column in MULTI_VALUE_COLUMNS:
            try:
                elements = split_multi(record[column])
            except ValueError as exc:
                issues.append(Issue(path, row_num, column, str(exc)))
                continue
            if len(elements) == 0:
                continue
            if "" in elements:
                issues.append(
                    Issue(
                        path, row_num, column, "multi-valued cell has an empty element"
                    )
                )
            if len(set(elements)) != len(elements):
                issues.append(
                    Issue(
                        path,
                        row_num,
                        column,
                        "multi-valued cell has duplicate elements",
                    )
                )

        # Row uniqueness.
        row_key = tuple(cells)
        if row_key in seen_rows:
            issues.append(
                Issue(
                    path,
                    row_num,
                    None,
                    f"exact duplicate of row {seen_rows[row_key]}",
                )
            )
        else:
            seen_rows[row_key] = row_num

        record_id = record["recordId"]
        if record_id != "":
            record_key = (
                record[framework_column],
                record["annex"],
                record["programKey"],
                record["measure"],
                record_id,
            )
            schemata = seen_record_ids.setdefault(record_key, {})
            if schema_name in schemata:
                issues.append(
                    Issue(
                        path,
                        row_num,
                        "recordId",
                        f"recordId {record_id!r} already used in row "
                        f"{schemata[schema_name]} for the same act, annex, "
                        "program, measure, and schema",
                    )
                )
            else:
                schemata[schema_name] = row_num

    # File-level CELEX consistency.
    if len(source_values) > 1:
        issues.append(
            Issue(
                path,
                None,
                source_column,
                f"rows disagree on the immediate source CELEX: {sorted(source_values)}",
            )
        )
    for value in sorted(source_values):
        if value != path.stem:
            issues.append(
                Issue(
                    path,
                    None,
                    source_column,
                    f"immediate source CELEX {value!r} does not match "
                    f"filename {path.name!r}",
                )
            )

    return ValidationResult(kind, records, issues)


@click.command(help="Validate reviewed EU Journal sanctions CSV files offline.")
@click.argument(
    "paths",
    nargs=-1,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
def cli(paths: tuple[Path, ...]) -> None:
    if len(paths) == 0:
        data_path = DATASET_DIR / "data"
        found = sorted(data_path.joinpath("amendments").glob("*.csv"))
        found.extend(sorted(data_path.joinpath("consolidated").glob("*.csv")))
        if len(found) == 0:
            raise click.UsageError(f"No CSV files found under {data_path}")
        paths = tuple(found)

    all_issues: list[Issue] = []
    for path in paths:
        result = validate_file(path)
        all_issues.extend(result.issues)
        if len(result.issues) == 0:
            click.echo(f"{path}: OK ({result.kind}, {len(result.rows)} rows)")
        else:
            click.echo(f"{path}: {len(result.issues)} issue(s)")
    for issue in sorted(all_issues, key=Issue.sort_key):
        click.echo(str(issue), err=True)
    if len(all_issues) > 0:
        sys.exit(1)


if __name__ == "__main__":
    cli()
