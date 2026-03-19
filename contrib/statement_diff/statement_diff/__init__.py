"""Command-line utilities for working with statements pack files."""

import logging
import math
from dataclasses import dataclass
from pathlib import Path
import click
from followthemoney import Statement
from followthemoney.statement.serialize import (
    CSV,
    PACK,
    read_path_statements,
)
from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.screen import ModalScreen
from textual.widgets import DataTable, Footer, Input, Static


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
    left = {sid: stmt for sid, stmt in left.items() if stmt.prop != "id"}
    right = {sid: stmt for sid, stmt in right.items() if stmt.prop != "id"}
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

    def _stmt_matches(self, stmt: Statement, query: str) -> bool:
        q = query.lower()
        return any(
            q in field.lower()
            for field in (
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
            )
        )

    def _build_match_indices(self) -> None:
        self._match_indices = [
            i
            for i, (_, stmt) in enumerate(self._result.rows)
            if self._stmt_matches(stmt, self._query)
        ]
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


def _read_stmts_file(path: Path) -> dict[str, Statement]:
    """Read a statements file, detecting format from the extension (.csv or pack)."""
    fmt = CSV if path.suffix.lower() == ".csv" else PACK
    stmts: dict[str, Statement] = {}
    for stmt in read_path_statements(path, fmt):
        if stmt.id is not None:
            stmts[stmt.id] = stmt
    return stmts


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


@click.group()
def cli() -> None:
    """Utilities for working with statements.pack files."""
    logging.basicConfig(level=logging.WARNING)


@cli.command("diff")
@click.argument(
    "left_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.argument(
    "right_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
def diff_cmd(left_path: Path, right_path: Path) -> None:
    """Diff two statements.pack (or .csv) files.

    \b
    Example:
        ftm-stmt diff ../data/tw_shtc-20231201-archive.pack ../data/tw_shtc-20240101.pack
    """
    click.echo(f"Loading {left_path}...", err=True)
    left_stmts = _read_stmts_file(left_path)
    click.echo(f"Loading {right_path}...", err=True)
    right_stmts = _read_stmts_file(right_path)
    _run_diff(left_stmts, right_stmts, str(left_path), str(right_path))


if __name__ == "__main__":
    cli()
