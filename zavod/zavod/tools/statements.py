"""Command-line utilities for working with statements pack files."""

import logging
import math
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import click
from followthemoney import Statement
from followthemoney.statement.serialize import (
    CSV,
    PACK,
    read_pack_statements_decoded,
    read_path_statements,
)
from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import DataTable, Footer, Static

from zavod.archive import (
    ARTIFACTS,
    STATEMENTS_FILE,
    dataset_resource_path,
    iter_dataset_versions,
    iter_previous_statements,
)
from zavod.archive.backend import ArchiveObject, get_archive_backend
from zavod.exc import ConfigurationException
from zavod.logs import configure_logging
from zavod.meta import load_dataset_from_path
from zavod.meta.dataset import Dataset
from zavod.runtime.versions import get_latest

# Column widths used when truncation or wrap mode is active
_COL_MARKER = 1
_COL_ENTITY = 50
_COL_SCHEMA = 20
_COL_PROP = 20
_COL_VALUE = 52
_COL_DATASET = 22
_COL_SEEN = 20  # first_seen and last_seen share this width
_COL_LANG = 5
_COL_ORIG_VALUE = 32
_COL_EXTERNAL = 3
_COL_ORIGIN = 20

_COLUMN_LABELS = (
    "",
    "ENTITY_ID",
    "SCHEMA",
    "PROP",
    "VALUE",
    "DATASET",
    "FIRST_SEEN",
    "LAST_SEEN",
    "LANG",
    "ORIG_VALUE",
    "EXTERNAL",
    "ORIGIN",
)
_COLUMN_WIDTHS = (
    _COL_MARKER,
    _COL_ENTITY,
    _COL_SCHEMA,
    _COL_PROP,
    _COL_VALUE,
    _COL_DATASET,
    _COL_SEEN,
    _COL_SEEN,
    _COL_LANG,
    _COL_ORIG_VALUE,
    _COL_EXTERNAL,
    _COL_ORIGIN,
)


@dataclass
class DiffResult:
    """Result of comparing two statement sets."""

    rows: list[tuple[str, Statement]]
    """Sorted (marker, stmt) pairs: marker is '-' for removed, '+' for added."""
    removed_count: int
    added_count: int
    unchanged_count: int


def compute_diff(
    left: dict[str, Statement],
    right: dict[str, Statement],
) -> DiffResult:
    """Compare two statement sets keyed by statement ID.

    Statements present only in `left` are marked as removed ('-'); those only in
    `right` are marked as added ('+').  Statements present in both are counted as
    unchanged and do not appear in `rows`.

    The rows are sorted by (entity_id, schema, prop, value) then by marker so that
    a removed and re-added statement appear as a '-'/'+' pair.
    """
    left_ids = set(left.keys())
    right_ids = set(right.keys())
    removed_ids = left_ids - right_ids
    added_ids = right_ids - left_ids

    rows: list[tuple[str, Statement]] = [("-", left[sid]) for sid in removed_ids] + [
        ("+", right[sid]) for sid in added_ids
    ]
    rows.sort(
        key=lambda x: (
            x[1].entity_id,
            x[1].schema,
            x[1].prop,
            x[1].value,
            0 if x[0] == "-" else 1,
        )
    )

    return DiffResult(
        rows=rows,
        removed_count=len(removed_ids),
        added_count=len(added_ids),
        unchanged_count=len(left_ids & right_ids),
    )


class _DiffApp(App[None]):
    """Textual TUI for browsing a statement diff."""

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("escape", "quit", "Quit"),
        Binding("t", "toggle_truncate", "Toggle truncation"),
        Binding("w", "toggle_wrap", "Toggle wrap"),
    ]

    CSS = """
    #summary {
        height: auto;
        padding: 0 1;
    }
    DataTable {
        height: 1fr;
    }
    """

    def __init__(
        self,
        left_label: str,
        right_label: str,
        result: DiffResult,
    ) -> None:
        super().__init__()
        self._left_label = left_label
        self._right_label = right_label
        self._result = result
        self._truncate = True
        self._wrap = False

    def compose(self) -> ComposeResult:
        summary = Text()
        summary.append(f"LEFT:  {self._left_label}\n", style="bold")
        summary.append(f"RIGHT: {self._right_label}\n", style="bold")
        summary.append(f"  {self._result.removed_count:,} removed", style="red")
        summary.append("  ")
        summary.append(f"{self._result.added_count:,} added", style="green")
        summary.append("  ")
        summary.append(f"{self._result.unchanged_count:,} unchanged", style="dim")
        yield Static(summary, id="summary")
        yield DataTable()
        yield Footer()

    def on_mount(self) -> None:
        self._populate_table()

    def _populate_table(self) -> None:
        table = self.query_one(DataTable)
        table.clear(columns=True)
        table.cursor_type = "row"
        # Fixed column widths in truncate/wrap modes; auto-size in full mode.
        if self._truncate or self._wrap:
            for label, width in zip(_COLUMN_LABELS, _COLUMN_WIDTHS):
                table.add_column(label, width=width)
        else:
            table.add_columns(*_COLUMN_LABELS)
        if not self._result.rows:
            table.add_row(
                "",
                Text("(no differences)", style="dim"),
                *("",) * (len(_COLUMN_LABELS) - 2),
            )
            return
        # In truncate/wrap modes non-value cells use ellipsis overflow on the fixed
        # column width; in full mode they expand freely.
        trunc = self._truncate or self._wrap

        def cell(s: str, style: str) -> Text:
            if trunc:
                return Text(s, style=style, no_wrap=True, overflow="ellipsis")
            return Text(s, style=style)

        for marker, stmt in self._result.rows:
            style = "red" if marker == "-" else "green"
            if self._wrap:
                value_cell = Text(stmt.value, style=style, overflow="fold")
                height = max(1, math.ceil(len(stmt.value) / _COL_VALUE))
            else:
                value_cell = cell(stmt.value, style)
                height = 1
            table.add_row(
                cell(marker, style),
                cell(stmt.entity_id, style),
                cell(stmt.schema, style),
                cell(stmt.prop, style),
                value_cell,
                cell(stmt.dataset, style),
                cell(stmt.first_seen or "", style),
                cell(stmt.last_seen or "", style),
                cell(stmt.lang or "", style),
                cell(stmt.original_value or "", style),
                cell("T" if stmt.external else "", style),
                cell(stmt.origin or "", style),
                height=height,
            )

    def action_toggle_truncate(self) -> None:
        if not self._wrap:
            self._truncate = not self._truncate
            self._populate_table()

    def action_toggle_wrap(self) -> None:
        self._wrap = not self._wrap
        self._populate_table()


def _read_pack_file(path: Path) -> dict[str, Statement]:
    """Read a pack statements file into a dict keyed by statement ID."""
    stmts: dict[str, Statement] = {}
    with open(path, "r") as fh:
        for stmt in read_pack_statements_decoded(fh):
            if stmt.id is not None:
                stmts[stmt.id] = stmt
    return stmts


def _read_local_statements(dataset: Dataset) -> dict[str, Statement]:
    """Read the local statements.pack for a dataset from the data directory."""
    path = dataset_resource_path(dataset.name, STATEMENTS_FILE)
    if not path.exists():
        raise click.ClickException(
            f"No local statements found for dataset: {dataset.name}\n"
            f"Expected at: {path}"
        )
    return _read_pack_file(path)


def _read_production_statements(dataset: Dataset) -> dict[str, Statement]:
    """Stream and collect the latest production (archive) statements for a dataset."""
    stmts: dict[str, Statement] = {}
    try:
        for stmt in iter_previous_statements(dataset, external=True):
            if stmt.id is not None:
                stmts[stmt.id] = stmt
    except ConfigurationException as exc:
        raise click.ClickException(
            f"Cannot load production statements: {exc}\n"
            "Ensure ZAVOD_ARCHIVE_BUCKET or ZAVOD_ARCHIVE_PATH is configured."
        ) from exc
    return stmts


def _read_stmts_file(path: Path) -> dict[str, Statement]:
    """Read a statements file, detecting format from the extension (.csv or pack)."""
    fmt = CSV if path.suffix.lower() == ".csv" else PACK
    stmts: dict[str, Statement] = {}
    for stmt in read_path_statements(path, fmt):
        if stmt.id is not None:
            stmts[stmt.id] = stmt
    return stmts


def _load_stmts_with_label(path: Path) -> tuple[dict[str, Statement], str]:
    """Load statements from a path.

    If the path is a .yml file that loads as a dataset, returns local statements
    for that dataset.  Otherwise reads the file directly as a statements file
    (pack format by default, CSV if the extension is .csv).
    """
    if path.suffix.lower() == ".yml":
        dataset = load_dataset_from_path(path)
        if dataset is not None:
            return _read_local_statements(dataset), f"local ({dataset.name})"
    return _read_stmts_file(path), str(path)


def _run_diff(
    left: dict[str, Statement],
    right: dict[str, Statement],
    left_label: str,
    right_label: str,
) -> None:
    """Compute and display the diff between two statement sets in a TUI."""
    result = compute_diff(left, right)
    app = _DiffApp(left_label=left_label, right_label=right_label, result=result)
    app.run()


def _get_production_pack(dataset_name: str) -> tuple[str, ArchiveObject]:
    """Return (version_id, archive_object) for the latest production statements.pack."""
    try:
        backend = get_archive_backend()
        for version in iter_dataset_versions(dataset_name):
            name = f"{ARTIFACTS}/{dataset_name}/{version.id}/{STATEMENTS_FILE}"
            obj = backend.get_object(name)
            if obj.exists():
                return version.id, obj
    except ConfigurationException as exc:
        raise click.ClickException(
            f"Cannot connect to archive: {exc}\n"
            "Ensure ZAVOD_ARCHIVE_BUCKET or ZAVOD_ARCHIVE_PATH is configured."
        ) from exc
    raise click.ClickException(
        f"No production statements found in archive for: {dataset_name}"
    )


@click.group()
def cli() -> None:
    """
    Utilities for working with statements.pack files.

    For example:

    - Compare a local statements.pack against the latest production version

    - Fetch a production statements.pack for quick local comparison

    - Copy your last local run's .pack to compare with subsequent runs

    - Compare two local statements.pack files against each other
    """
    configure_logging(level=logging.WARNING)


@cli.command("cp")
@click.argument(
    "dataset_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.argument("dest_dir", type=click.Path(file_okay=False, path_type=Path))
def cp_cmd(dataset_path: Path, dest_dir: Path) -> None:
    """
    Copy statements.pack for a dataset to <dest_dir>/<dataset_name>-<version_id>.pack

    \b
    Example:
        zavod-stmt cp datasets/tw/shtc/tw_shtc.yml ../data
    """
    dataset = load_dataset_from_path(dataset_path)
    if dataset is None:
        raise click.BadParameter(f"Invalid dataset path: {dataset_path}")

    version = get_latest(dataset.name, backfill=False)
    if version is None:
        raise click.ClickException(
            f"No local version found for dataset: {dataset.name}"
        )

    src = dataset_resource_path(dataset.name, STATEMENTS_FILE)
    if not src.exists():
        raise click.ClickException(
            f"No local statements file found for dataset: {dataset.name}\n"
            f"Expected at: {src}"
        )

    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{dataset.name}-{version.id}.pack"
    shutil.copy2(src, dest)
    click.echo(f"Copied: {src}")
    click.echo(f"    to: {dest}")


@cli.command("fetch")
@click.argument(
    "dataset_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.argument("dest_dir", type=click.Path(file_okay=False, path_type=Path))
def fetch_cmd(dataset_path: Path, dest_dir: Path) -> None:
    """
    Download the latest production statements pack for a dataset.

    Saves to <dest_dir>/<dataset_name>-<version_id>-archive.pack.

    \b
    Example:
        zavod-stmt fetch datasets/tw/shtc/tw_shtc.yml ../data
    """
    dataset = load_dataset_from_path(dataset_path)
    if dataset is None:
        raise click.BadParameter(f"Invalid dataset path: {dataset_path}")

    click.echo(f"Finding latest production version for {dataset.name}...", err=True)
    version_id, obj = _get_production_pack(dataset.name)

    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{dataset.name}-{version_id}-archive.pack"
    click.echo(f"Downloading {obj.name}...", err=True)
    obj.backfill(dest)
    click.echo(f"Saved to: {dest}")


@cli.command("diff")
@click.argument(
    "left_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.argument(
    "right_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=False,
    default=None,
)
def diff_cmd(
    left_path: Path,
    right_path: Optional[Path],
) -> None:
    """Diff two statement sets. Arguments can be .yml dataset paths or .pack files.

    With one argument (must be a .yml),
    diffs local statements against the latest production version.

    With two arguments (each can be a dataset .yml or a statements.pack file),
    diffs the left statement set against the right.

    \b
    Examples:
        zavod-stmt diff datasets/tw/shtc/tw_shtc.yml
        zavod-stmt diff ../data/tw_shtc-20240101.pack datasets/tw/shtc/tw_shtc.yml
        zavod-stmt diff ../data/tw_shtc-20231201-archive.pack ../data/tw_shtc-20240101.pack
    """
    if right_path is None:
        if left_path.suffix != ".yml":
            raise click.UsageError(
                "When providing a single argument, it must be a .yml dataset path "
                "(to diff local statements against production)."
            )
        dataset = load_dataset_from_path(left_path)
        if dataset is None:
            raise click.BadParameter(f"Invalid dataset path: {left_path}")
        click.echo(f"Loading local statements for {dataset.name}...", err=True)
        left_stmts = _read_local_statements(dataset)
        click.echo(f"Loading production statements for {dataset.name}...", err=True)
        right_stmts = _read_production_statements(dataset)
        left_label = f"local ({dataset.name})"
        right_label = f"production ({dataset.name})"
    else:
        click.echo(f"Loading {left_path}...", err=True)
        left_stmts, left_label = _load_stmts_with_label(left_path)
        click.echo(f"Loading {right_path}...", err=True)
        right_stmts, right_label = _load_stmts_with_label(right_path)

    _run_diff(left_stmts, right_stmts, left_label, right_label)


if __name__ == "__main__":
    cli()
