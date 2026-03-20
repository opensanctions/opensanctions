"""Command-line utilities for working with statements pack files."""

import logging
import math
import tempfile
import time
from pathlib import Path
from typing import IO, Any, cast

import click
import fsspec
import pandas as pd
from followthemoney.statement.serialize import (
    CSV,
    PACK,
    read_path_statements,
)
from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)
from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.screen import ModalScreen
from textual.widgets import DataTable, Footer, Input, Static

log = logging.getLogger(__name__)


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


# DataFrame columns used for display/search (excludes the internal _stmt column)
_DF_TEXT_COLS = [
    "entity_id",
    "schema",
    "prop",
    "value",
    "dataset",
    "first_seen",
    "last_seen",
    "lang",
    "original_value",
    "origin",
]


class DiffResult:
    """Result of comparing two statement sets.

    Internally backed by a pandas DataFrame for fast sorting and searching.
    The ``rows`` property exposes the data as a list of ``(marker, row)``
    named-tuple pairs for backward compatibility.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        removed_count: int,
        added_count: int,
        unchanged_count: int,
    ) -> None:
        self.df = df
        self.removed_count = removed_count
        self.added_count = added_count
        self.unchanged_count = unchanged_count

    @property
    def rows(self) -> list[tuple[str, Any]]:
        """Sorted (marker, row) pairs: marker is '-' for removed, '+' for added.

        Each row is a named tuple with fields matching ``_DF_TEXT_COLS`` plus
        ``external`` and ``marker``, supporting attribute access (e.g. row.value).
        """
        if self.df.empty:
            return []
        return [(r.marker, r) for r in self.df.itertuples(index=False)]


def compute_diff(left: pd.DataFrame, right: pd.DataFrame) -> DiffResult:
    """Compare two statement DataFrames indexed by statement ID.

    Statements present only in ``left`` are marked as removed ('-'); those only
    in ``right`` are marked as added ('+').  Statements present in both are
    counted as unchanged and do not appear in ``rows``.

    The rows are sorted by (entity_id, schema, prop, value) then by marker so
    that a removed and re-added statement appear as a '-'/'+' pair.
    """
    left = left[left["prop"] != "id"]
    right = right[right["prop"] != "id"]

    t0 = time.perf_counter()
    removed = left[~left.index.isin(right.index)].copy()
    added = right[~right.index.isin(left.index)].copy()
    unchanged_count = int(left.index.isin(right.index).sum())
    log.info(
        "diff set ops: %d removed, %d added, %d unchanged in %.3fs",
        len(removed),
        len(added),
        unchanged_count,
        time.perf_counter() - t0,
    )

    removed["marker"] = "-"
    added["marker"] = "+"

    if len(removed) + len(added) > 0:
        t1 = time.perf_counter()
        df = pd.concat([removed, added], ignore_index=True)
        df["_marker_ord"] = (df["marker"] == "+").astype("int8")
        df.sort_values(
            ["entity_id", "schema", "prop", "value", "_marker_ord"],
            inplace=True,
            ignore_index=True,
        )
        df.drop(columns=["_marker_ord"], inplace=True)
        # Ensure marker column comes first for readability
        df = df[["marker", *_DF_TEXT_COLS, "external"]]
        log.info("diff sort+concat: %.3fs", time.perf_counter() - t1)
    else:
        df = pd.DataFrame(columns=["marker", *_DF_TEXT_COLS, "external"])

    return DiffResult(
        df=df,
        removed_count=len(removed),
        added_count=len(added),
        unchanged_count=unchanged_count,
    )


class _SearchScreen(ModalScreen[str | None]):
    """Transparent modal overlay for entering a search query."""

    CSS = """
    _SearchScreen {
        background: transparent;
        align: left bottom;
    }
    _SearchScreen > Input {
        dock: bottom;
        border: tall $accent;
        background: $surface;
        color: $text;
        width: 1fr;
    }
    """

    BINDINGS = [Binding("escape", "cancel", "Cancel", show=False)]

    def compose(self) -> ComposeResult:
        yield Input(placeholder="/search…")

    def on_mount(self) -> None:
        self.query_one(Input).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.dismiss(event.value.strip() or None)

    def action_cancel(self) -> None:
        self.dismiss(None)


class _DiffApp(App[None]):
    """Textual TUI for browsing a statement diff."""

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("escape", "quit", "Quit", show=False),
        Binding("t", "toggle_truncate", "Truncation"),
        Binding("w", "toggle_wrap", "Wrap"),
        Binding("/", "search", "Search"),
        Binding("n", "next_match", "Next match"),
        Binding("N", "prev_match", "Prev match"),
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
        self._query = ""
        self._match_indices: list[int] = []
        self._match_pos = -1

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

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def _build_match_indices(self) -> None:
        q = self._query.lower()
        df = self._result.df
        mask = pd.Series(False, index=df.index)
        for col in _DF_TEXT_COLS:
            mask |= df[col].str.lower().str.contains(q, regex=False, na=False)
        self._match_indices = df.index[mask].tolist()
        self._match_pos = -1

    def _jump_to_match(self, pos: int) -> None:
        if not self._match_indices:
            self.notify("No matches", severity="warning")
            return
        self._match_pos = pos % len(self._match_indices)
        self.query_one(DataTable).move_cursor(row=self._match_indices[self._match_pos])

    def action_search(self) -> None:
        def _on_dismiss(query: str | None) -> None:
            if query:
                self._query = query
                self._build_match_indices()
                self._jump_to_match(0)

        self.push_screen(_SearchScreen(), _on_dismiss)

    def action_next_match(self) -> None:
        if self._match_indices:
            self._jump_to_match(self._match_pos + 1)

    def action_prev_match(self) -> None:
        if self._match_indices:
            self._jump_to_match(self._match_pos - 1)

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
        if self._result.df.empty:
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

        t0 = time.perf_counter()
        for row in self._result.df.itertuples(index=False):
            marker: str = row.marker
            style = "red" if marker == "-" else "green"
            value: str = row.value
            if self._wrap:
                value_cell = Text(value, style=style, overflow="fold")
                height = max(1, math.ceil(len(value) / _COL_VALUE))
            else:
                value_cell = cell(value, style)
                height = 1
            table.add_row(
                cell(marker, style),
                cell(row.entity_id, style),
                cell(row.schema, style),
                cell(row.prop, style),
                value_cell,
                cell(row.dataset, style),
                cell(row.first_seen, style),
                cell(row.last_seen, style),
                cell(row.lang, style),
                cell(row.original_value, style),
                cell("T" if row.external else "", style),
                cell(row.origin, style),
                height=height,
            )
        log.info(
            "built table (%d rows) in %.3fs",
            len(self._result.df),
            time.perf_counter() - t0,
        )

    def action_toggle_truncate(self) -> None:
        if not self._wrap:
            self._truncate = not self._truncate
            self._populate_table()

    def action_toggle_wrap(self) -> None:
        self._wrap = not self._wrap
        self._populate_table()


def _stmts_to_df(path: Path, fmt: object) -> pd.DataFrame:
    """Read statements from a path and return a DataFrame indexed by statement ID."""
    t0 = time.perf_counter()
    records = []
    for stmt in read_path_statements(path, fmt):
        if stmt.id is not None:
            records.append(
                (
                    stmt.id,
                    stmt.entity_id,
                    stmt.schema,
                    stmt.prop,
                    stmt.value,
                    stmt.dataset,
                    stmt.first_seen or "",
                    stmt.last_seen or "",
                    stmt.lang or "",
                    stmt.original_value or "",
                    stmt.origin or "",
                    stmt.external,
                )
            )
    log.info("parsed %d statements in %.3fs", len(records), time.perf_counter() - t0)
    t1 = time.perf_counter()
    df = pd.DataFrame(
        records,
        columns=["stmt_id", *_DF_TEXT_COLS, "external"],
    )
    df.set_index("stmt_id", inplace=True)
    # Keep only the last statement seen per ID (mirrors original dict behaviour)
    df = df[~df.index.duplicated(keep="last")]
    log.info("built DataFrame (%d rows) in %.3fs", len(df), time.perf_counter() - t1)
    return df


def _read_stmts_file(url: str) -> pd.DataFrame:
    """Read statements from a local path or remote URL (.pack or .csv)."""
    fmt = CSV if url.lower().endswith(".csv") else PACK
    if "://" not in url:
        return _stmts_to_df(Path(url), fmt)
    suffix = ".csv" if fmt == CSV else ".pack"
    # block_size=0 enables streaming for servers that don't support range requests
    with fsspec.open(url, "rb", block_size=0) as f:
        f = cast(IO[bytes], f)
        size = getattr(f, "size", None)
        with (
            tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp,
            Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                DownloadColumn(),
                TimeRemainingColumn(),
                console=Console(stderr=True),
                transient=True,
            ) as progress,
        ):
            tmp_path = Path(tmp.name)
            task = progress.add_task(url, total=size)
            while chunk := f.read(65536):
                tmp.write(chunk)
                progress.advance(task, len(chunk))
    try:
        return _stmts_to_df(tmp_path, fmt)
    finally:
        tmp_path.unlink(missing_ok=True)


@click.group()
def cli() -> None:
    """Utilities for working with statements.pack files."""
    logging.basicConfig(level=logging.INFO, format="%(name)s %(levelname)s %(message)s")


@cli.command("diff")
@click.argument("left_path")
@click.argument("right_path")
@click.option(
    "--output",
    "-o",
    type=click.Choice(["tui", "csv"]),
    default="tui",
    help="Output format: interactive TUI (default) or CSV to stdout.",
)
def diff_cmd(left_path: str, right_path: str, output: str) -> None:
    """Diff two statements.pack (or .csv) files. Accepts local paths or https:// URLs.

    \b
    Example:
        ftm-stmt diff ../data/tw_shtc-20231201-archive.pack ../data/tw_shtc-20240101.pack
        ftm-stmt diff https://data.opensanctions.org/.../statements.pack ./local.pack
        ftm-stmt diff -o csv left.pack right.pack | csvlens
    """
    click.echo(f"Loading {left_path}...", err=True)
    left_stmts = _read_stmts_file(left_path)
    click.echo(f"Loading {right_path}...", err=True)
    right_stmts = _read_stmts_file(right_path)
    result = compute_diff(left_stmts, right_stmts)
    if output == "csv":
        import sys

        result.df.to_csv(sys.stdout, index=False)
    else:
        app = _DiffApp(
            left_label=str(left_path),
            right_label=str(right_path),
            result=result,
        )
        app.run()


if __name__ == "__main__":
    cli()
