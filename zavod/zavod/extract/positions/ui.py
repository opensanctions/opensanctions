import getpass
from collections.abc import Callable
from datetime import UTC, datetime

from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import Screen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Label,
    SelectionList,
    Static,
)

from models import (
    LEVELS,
    ROLES,
    SENIORITIES,
    Annotation,
    AnnotationRecord,
    GoldenAnnotation,
    HumanAnnotation,
    Level,
    PrimaryAnnotation,
    Seniority,
)

TAGGER_LEVELS: tuple[Level, ...] = tuple(
    level for level in LEVELS if level not in ("none", "undecided")
)
TAGGER_SENIORITIES: tuple[Seniority, ...] = tuple(
    seniority for seniority in SENIORITIES if seniority != "undecided"
)

STATE_STYLES = {
    "pending": "dim",
    "approved": "green",
    "vetoed": "red",
    "undecided": "magenta",
    "human": "blue",
    "golden": "yellow",
}


def record_state(record: AnnotationRecord) -> str:
    if record.golden() is not None:
        return "golden"
    primary = record.latest_primary()
    if primary is None or primary.review is None:
        return "pending"
    if record.human_on(primary) is not None:
        return "human"
    if primary.review.veto:
        return "vetoed"
    if primary.is_undecided():
        return "undecided"
    return "approved"


def needs_human(record: AnnotationRecord) -> bool:
    return record_state(record) in ("vetoed", "undecided")


def editable_annotation(record: AnnotationRecord) -> Annotation | None:
    """The annotation that the tagger shows and a save replaces."""
    golden = record.golden()
    if golden is not None:
        return golden
    primary = record.latest_primary()
    if primary is None:
        return None
    human = record.human_on(primary)
    return human if human is not None else primary


def format_annotation(annotation: Annotation) -> Table:
    table = Table.grid(padding=(0, 2))
    table.add_row("Level", str(annotation.level))
    table.add_row("Roles", ", ".join(annotation.roles) or "[dim]undecided[/dim]")
    table.add_row("Seniority", str(annotation.seniority))
    return table


def render_record(record: AnnotationRecord) -> Group:
    item = record.item
    item_table = Table.grid(padding=(0, 2))
    item_table.add_row("Title", Text(item.caption, style="bold"))
    item_table.add_row("Countries", ", ".join(item.countries) or "-")
    item_table.add_row("Subnational", "; ".join(item.subnational_areas) or "-")
    item_table.add_row("Dataset", item.dataset)
    item_table.add_row("ID", str(record.id))
    state = record_state(record)
    item_table.add_row("State", Text(state, style=STATE_STYLES[state]))

    panels: list[Panel] = [Panel(item_table, title="Position")]
    for annotation in record.annotations:
        panels.extend(annotation_panels(annotation))
    return Group(*panels)


def annotation_panels(
    annotation: GoldenAnnotation | PrimaryAnnotation | HumanAnnotation,
) -> list[Panel]:
    if isinstance(annotation, GoldenAnnotation):
        body = Group(format_annotation(annotation), "", Text(annotation.note))
        return [Panel(body, title="Golden", border_style="yellow")]
    if isinstance(annotation, HumanAnnotation):
        parts: list[Table | Text | str] = [format_annotation(annotation)]
        if annotation.note is not None:
            parts.extend(["", Text(annotation.note)])
        return [
            Panel(
                Group(*parts),
                title=f"Human — {annotation.author}",
                border_style="blue",
            )
        ]
    panels = [
        Panel(
            Group(
                format_annotation(annotation),
                "",
                Text("Key evidence", style="bold"),
                Text(annotation.key_evidence),
                "",
                Text("Reasoning", style="bold"),
                Text(annotation.reasoning),
            ),
            title=f"Annotation — {annotation.model}",
            border_style="cyan",
        )
    ]
    review = annotation.review
    if review is not None:
        verdict = "Veto" if review.veto else "Approval"
        panels.append(
            Panel(
                Text(review.reasoning),
                title=f"{verdict} — {review.model}",
                border_style="red" if review.veto else "green",
            )
        )
    return panels


class SingleSelectionList[T: str](SelectionList[T]):
    """A selection list that holds at most one selected value."""

    def __init__(self, values: tuple[T, ...], id: str) -> None:
        super().__init__(*((value, value) for value in values), id=id)
        self.values = values

    def on_selection_list_selection_toggled(
        self, event: SelectionList.SelectionToggled[T]
    ) -> None:
        value = event.selection.value
        if value in self.selected:
            for other in self.selected:
                if other != value:
                    self.deselect(other)

    def load(self, value: T) -> None:
        self.deselect_all()
        if value in self.values:
            self.select(value)

    def value(self) -> T | None:
        return self.selected[0] if self.selected else None


class Tagger(Horizontal):
    """One column per dimension, holding the human decision on a record.

    A dimension without a selection is saved as 'undecided'."""

    DEFAULT_CSS = """
    Tagger { height: auto; border-top: solid $accent; }
    Tagger > Vertical { width: 1fr; height: auto; padding-right: 1; }
    Tagger Label { text-style: bold; }
    Tagger SelectionList { height: auto; max-height: 13; }
    Tagger #actions { width: auto; padding-right: 0; }
    """

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Label("Level")
            yield SingleSelectionList(TAGGER_LEVELS, id="level")
        with Vertical():
            yield Label("Roles")
            yield SelectionList[str](*((role, role) for role in ROLES), id="roles")
        with Vertical():
            yield Label("Seniority")
            yield SingleSelectionList(TAGGER_SENIORITIES, id="seniority")
        with Vertical(id="actions"):
            yield Button("Save (ctrl+s)", variant="primary", id="save")

    def load(self, annotation: Annotation | None) -> None:
        self.disabled = annotation is None
        if annotation is None:
            return
        self.level_list.load(annotation.level)
        self.seniority_list.load(annotation.seniority)
        roles = self.query_one("#roles", SelectionList)
        roles.deselect_all()
        for role in annotation.roles:
            roles.select(role)

    @property
    def level_list(self) -> SingleSelectionList[Level]:
        return self.query_one("#level", SingleSelectionList)

    @property
    def seniority_list(self) -> SingleSelectionList[Seniority]:
        return self.query_one("#seniority", SingleSelectionList)

    def read(self) -> Annotation:
        selected = self.query_one("#roles", SelectionList).selected
        return Annotation(
            level=self.level_list.value() or "undecided",
            roles=[role for role in ROLES if role in selected],
            seniority=self.seniority_list.value() or "undecided",
        )


class ListScreen(Screen[None]):
    BINDINGS = [
        Binding("n", "next", "Next for human"),
        Binding("p", "previous", "Previous for human"),
        Binding("ctrl+s", "save", "Save"),
        Binding("q", "app.quit", "Quit"),
    ]
    DEFAULT_CSS = """
    #records { height: 1fr; }
    #detail { height: 1fr; border-top: solid $accent; }
    """

    def __init__(
        self, records: list[AnnotationRecord], on_change: Callable[[], None]
    ) -> None:
        super().__init__()
        self.records = records
        self.on_change = on_change

    def compose(self) -> ComposeResult:
        yield Header()
        yield DataTable(id="records", cursor_type="row", zebra_stripes=True)
        with VerticalScroll(id="detail"):
            yield Static(id="record")
        yield Tagger()
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#records", DataTable)
        table.add_column("#")
        table.add_column("Title")
        table.add_column("Location")
        table.add_column("Dataset")
        for key in ("State", "Level", "Roles", "Seniority"):
            table.add_column(key, key=key)
        for index, record in enumerate(self.records):
            table.add_row(
                str(index + 1),
                record.item.caption[:80],
                ", ".join(record.item.countries + record.item.subnational_areas),
                record.item.dataset,
                *self.status_cells(record),
                key=str(index),
            )
        self.update_subtitle()
        table.focus()
        if self.records and not needs_human(self.records[0]):
            self.action_next()

    @staticmethod
    def status_cells(record: AnnotationRecord) -> list[Text | str]:
        state = record_state(record)
        annotation = record.final_annotation() or record.latest_primary()
        if annotation is None:
            labels = ["", "", ""]
        else:
            labels = [
                str(annotation.level),
                ", ".join(annotation.roles),
                str(annotation.seniority),
            ]
        return [Text(state, style=STATE_STYLES[state]), *labels]

    def update_subtitle(self) -> None:
        open_count = sum(1 for r in self.records if needs_human(r))
        self.sub_title = f"{len(self.records)} records, {open_count} for human"

    @property
    def record(self) -> AnnotationRecord:
        return self.records[self.query_one("#records", DataTable).cursor_row]

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        record = self.records[event.cursor_row]
        self.query_one("#record", Static).update(render_record(record))
        self.query_one(Tagger).load(editable_annotation(record))

    def move_to_human(self, step: int) -> None:
        table = self.query_one("#records", DataTable)
        count = len(self.records)
        for offset in range(1, count + 1):
            index = (table.cursor_row + step * offset) % count
            if needs_human(self.records[index]):
                table.move_cursor(row=index)
                return
        self.notify("No records left for a human.")

    def action_next(self) -> None:
        self.move_to_human(1)

    def action_previous(self) -> None:
        self.move_to_human(-1)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "save":
            self.action_save()

    def action_save(self) -> None:
        tagger = self.query_one(Tagger)
        if tagger.disabled:
            return
        record = self.record
        golden = record.golden()
        if golden is not None:
            # A golden record is a reference: edits replace it rather than layer on it.
            index = record.annotations.index(golden)
            record.annotations[index] = GoldenAnnotation(
                **tagger.read().model_dump(), note=golden.note
            )
        else:
            primary = record.latest_primary()
            assert primary is not None
            previous = record.human_on(primary)
            if previous is not None:
                record.annotations.remove(previous)
            record.annotations.append(
                HumanAnnotation(
                    **tagger.read().model_dump(),
                    created_at=datetime.now(UTC),
                    author=getpass.getuser(),
                    target=primary.id,
                )
            )
        self.on_change()
        table = self.query_one("#records", DataTable)
        row_key = str(table.cursor_row)
        for key, value in zip(
            ("State", "Level", "Roles", "Seniority"), self.status_cells(record)
        ):
            table.update_cell(row_key, key, value)
        self.query_one("#record", Static).update(render_record(record))
        self.update_subtitle()
        self.notify("Saved.")


class ListApp(App[None]):
    TITLE = "Annotated positions"

    def __init__(
        self, records: list[AnnotationRecord], on_change: Callable[[], None]
    ) -> None:
        super().__init__()
        self.records = records
        self.on_change = on_change

    def on_mount(self) -> None:
        self.push_screen(ListScreen(self.records, self.on_change))
