"""Command-line utilities for working with statements pack files."""

import logging
import shutil
import textwrap
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

# Default column widths for truncation mode
_COL_ENTITY = 50
_COL_PROP = 32
_COL_VALUE = 52
_COL_DATASET = 22
_COL_SEEN = 12


def _trunc(s: str, width: int) -> str:
    """Truncate a string to at most `width` characters, appending an ellipsis if cut."""
    if len(s) > width:
        return s[: width - 1] + "\u2026"
    return s


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
        diff_rows: list[tuple[str, Statement]],
        removed_count: int,
        added_count: int,
        common_count: int,
    ) -> None:
        super().__init__()
        self._left_label = left_label
        self._right_label = right_label
        self._diff_rows = diff_rows
        self._removed_count = removed_count
        self._added_count = added_count
        self._common_count = common_count
        self._truncate = True
        self._wrap = False

    def compose(self) -> ComposeResult:
        summary = Text()
        summary.append(f"LEFT:  {self._left_label}\n", style="bold")
        summary.append(f"RIGHT: {self._right_label}\n", style="bold")
        summary.append(f"  {self._removed_count:,} removed", style="red")
        summary.append("  ")
        summary.append(f"{self._added_count:,} added", style="green")
        summary.append("  ")
        summary.append(f"{self._common_count:,} unchanged", style="dim")
        yield Static(summary, id="summary")
        yield DataTable()
        yield Footer()

    def on_mount(self) -> None:
        self._populate_table()

    def _populate_table(self) -> None:
        table = self.query_one(DataTable)
        table.clear(columns=True)
        table.cursor_type = "row"
        table.add_columns(
            "", "ENTITY_ID", "SCHEMA:PROP", "VALUE", "DATASET", "FIRST_SEEN"
        )
        if not self._diff_rows:
            table.add_row("", Text("(no differences)", style="dim"), "", "", "", "")
            return
        for marker, stmt in self._diff_rows:
            style = "red" if marker == "-" else "green"
            prop_col = f"{stmt.schema}:{stmt.prop}"
            if self._wrap:
                value_lines = textwrap.wrap(stmt.value, _COL_VALUE) or [""]
                value_cell = Text("\n".join(value_lines), style=style)
                table.add_row(
                    Text(marker, style=style),
                    Text(_trunc(stmt.entity_id, _COL_ENTITY), style=style),
                    Text(_trunc(prop_col, _COL_PROP), style=style),
                    value_cell,
                    Text(_trunc(stmt.dataset, _COL_DATASET), style=style),
                    Text(_trunc(stmt.first_seen or "", _COL_SEEN), style=style),
                    height=len(value_lines),
                )
            elif self._truncate:
                table.add_row(
                    Text(marker, style=style),
                    Text(_trunc(stmt.entity_id, _COL_ENTITY), style=style),
                    Text(_trunc(prop_col, _COL_PROP), style=style),
                    Text(_trunc(stmt.value, _COL_VALUE), style=style),
                    Text(_trunc(stmt.dataset, _COL_DATASET), style=style),
                    Text(_trunc(stmt.first_seen or "", _COL_SEEN), style=style),
                )
            else:
                table.add_row(
                    Text(marker, style=style),
                    Text(stmt.entity_id, style=style),
                    Text(prop_col, style=style),
                    Text(stmt.value, style=style),
                    Text(stmt.dataset, style=style),
                    Text(stmt.first_seen or "", style=style),
                )

    def action_toggle_truncate(self) -> None:
        if not self._wrap:
            self._truncate = not self._truncate
            self._populate_table()

    def action_toggle_wrap(self) -> None:
        self._wrap = not self._wrap
        self._populate_table()


def _stmt_sort_key(stmt: Statement) -> tuple[str, str, str, str]:
    return (stmt.entity_id, stmt.schema, stmt.prop, stmt.value)


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
    left_ids = set(left.keys())
    right_ids = set(right.keys())
    removed_ids = left_ids - right_ids
    added_ids = right_ids - left_ids
    common_count = len(left_ids & right_ids)

    removed: list[tuple[str, Statement]] = [("-", left[sid]) for sid in removed_ids]
    added: list[tuple[str, Statement]] = [("+", right[sid]) for sid in added_ids]
    all_diffs = removed + added
    # Sort by content, with "-" (removed) before "+" (added) for the same statement key
    all_diffs.sort(key=lambda x: (_stmt_sort_key(x[1]), x[0]))

    app = _DiffApp(
        left_label=left_label,
        right_label=right_label,
        diff_rows=all_diffs,
        removed_count=len(removed_ids),
        added_count=len(added_ids),
        common_count=common_count,
    )
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


@click.group(help="Utilities for working with statements pack files.")
def cli() -> None:
    configure_logging(level=logging.WARNING)


@cli.command(
    "cp", help="Copy the local statements pack file for a dataset, named by version."
)
@click.argument(
    "dataset_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.argument("dest_dir", type=click.Path(file_okay=False, path_type=Path))
def cp_cmd(dataset_path: Path, dest_dir: Path) -> None:
    """Copy statements.pack to <dest_dir>/<dataset_name>-<version_id>.pack."""
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


@cli.command(
    "fetch",
    help="Download the latest production statements pack for a dataset.",
)
@click.argument(
    "dataset_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.argument("dest_dir", type=click.Path(file_okay=False, path_type=Path))
def fetch_cmd(dataset_path: Path, dest_dir: Path) -> None:
    """Download statements.pack from production to <dest_dir>/<dataset_name>-<version_id>-archive.pack."""
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


@cli.command(
    "diff", help="Show a diff of two statement sets in a scrollable TUI table."
)
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

    \b
    With one argument (must be a .yml):
        Diffs local statements against the latest production version.

    With two arguments (each can be a .yml or a .pack file):
        Diffs the left statement set against the right.
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
