"""Command-line utilities for working with statements pack files."""

import logging
import shutil
import subprocess
from pathlib import Path
from typing import Optional

import click
from colorama import Fore, Style  # type: ignore[import-untyped]
from colorama import init as colorama_init
from followthemoney import Statement
from followthemoney.statement.serialize import read_pack_statements_decoded

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

colorama_init()

# Column widths for the tabular diff display
COL_ENTITY = 42
COL_PROP = 32
COL_VALUE = 52
COL_DATASET = 22
COL_SEEN = 10

_SEP_WIDTH = (
    2 + COL_ENTITY + 2 + COL_PROP + 2 + COL_VALUE + 2 + COL_DATASET + 2 + COL_SEEN
)


def _trunc(s: str, width: int) -> str:
    """Truncate a string to a fixed width, padding with spaces on the right."""
    if len(s) > width:
        return s[: width - 1] + "\u2026"
    return s.ljust(width)


def _format_header() -> str:
    cols = [
        _trunc("ENTITY_ID", COL_ENTITY),
        _trunc("SCHEMA:PROP", COL_PROP),
        _trunc("VALUE", COL_VALUE),
        _trunc("DATASET", COL_DATASET),
        _trunc("FIRST_SEEN", COL_SEEN),
    ]
    return str(Style.BRIGHT + "  " + "  ".join(cols) + Style.RESET_ALL)


def _format_separator() -> str:
    return str(Style.DIM + ("\u2500" * _SEP_WIDTH) + Style.RESET_ALL)


def _format_row(marker: str, stmt: Statement) -> str:
    color = Fore.RED if marker == "-" else Fore.GREEN
    prop_col = f"{stmt.schema}:{stmt.prop}"
    cols = [
        _trunc(stmt.entity_id, COL_ENTITY),
        _trunc(prop_col, COL_PROP),
        _trunc(stmt.value, COL_VALUE),
        _trunc(stmt.dataset, COL_DATASET),
        _trunc(stmt.first_seen or "", COL_SEEN),
    ]
    return str(color + marker + " " + "  ".join(cols) + Style.RESET_ALL)


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


def _load_stmts_with_label(path: Path) -> tuple[dict[str, Statement], str]:
    """Load statements from a .yml dataset path or a .pack file, with a display label."""
    if path.suffix == ".yml":
        dataset = load_dataset_from_path(path)
        if dataset is None:
            raise click.BadParameter(f"Invalid dataset path: {path}")
        return _read_local_statements(dataset), f"local ({dataset.name})"
    return _read_pack_file(path), str(path)


def _display_in_pager(lines: list[str], wrap: bool) -> None:
    """Write lines to a pager. Horizontal scrolling is on by default; --wrap enables wrapping."""
    content = "\n".join(lines) + "\n"
    # -R: render ANSI colour codes. -S: chop long lines (enables horizontal scrolling).
    pager_args = ["less", "-R"] if wrap else ["less", "-RS"]
    try:
        proc = subprocess.Popen(
            pager_args,
            stdin=subprocess.PIPE,
            encoding="utf-8",
        )
        proc.communicate(content)
    except (BrokenPipeError, KeyboardInterrupt):
        pass
    except FileNotFoundError:
        # less is not available; fall back to stdout
        click.echo(content)


def _run_diff(
    left: dict[str, Statement],
    right: dict[str, Statement],
    left_label: str,
    right_label: str,
    wrap: bool,
) -> None:
    """Compute and display the diff between two statement sets in a pager."""
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

    lines: list[str] = []
    lines.append(Style.BRIGHT + f"LEFT:  {left_label}" + Style.RESET_ALL)
    lines.append(Style.BRIGHT + f"RIGHT: {right_label}" + Style.RESET_ALL)
    lines.append(
        Fore.RED
        + f"  {len(removed_ids):,} removed"
        + Style.RESET_ALL
        + "  "
        + Fore.GREEN
        + f"{len(added_ids):,} added"
        + Style.RESET_ALL
        + "  "
        + Style.DIM
        + f"{common_count:,} unchanged"
        + Style.RESET_ALL
    )
    lines.append(_format_separator())
    lines.append(_format_header())
    lines.append(_format_separator())

    if not all_diffs:
        lines.append(Style.DIM + "  (no differences)" + Style.RESET_ALL)
    else:
        for marker, stmt in all_diffs:
            lines.append(_format_row(marker, stmt))

    lines.append(_format_separator())
    _display_in_pager(lines, wrap)


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


@cli.command("diff", help="Show a diff of two statement sets in a pager.")
@click.argument(
    "left_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.argument(
    "right_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=False,
    default=None,
)
@click.option(
    "-w",
    "--wrap",
    is_flag=True,
    default=False,
    help="Enable line wrapping (default: disabled, with horizontal scrolling).",
)
def diff_cmd(
    left_path: Path,
    right_path: Optional[Path],
    wrap: bool,
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

    _run_diff(left_stmts, right_stmts, left_label, right_label, wrap)


if __name__ == "__main__":
    cli()
