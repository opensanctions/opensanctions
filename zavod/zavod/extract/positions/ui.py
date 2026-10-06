from collections.abc import Callable

from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import ModalScreen, Screen
from textual.widgets import (
    Button,
    Checkbox,
    DataTable,
    Footer,
    Header,
    Label,
    RadioButton,
    RadioSet,
    SelectionList,
    Static,
)

from models import LEVELS, ROLES, SENIORITIES, Annotation, AnnotationRecord

STATE_STYLES = {"pending": "dim", "approved": "green", "vetoed": "red", "human": "blue"}


def record_state(record: AnnotationRecord) -> str:
    if record.review is None:
        return "pending"
    if not record.review.veto:
        return "approved"
    if record.human is None:
        return "vetoed"
    return "human"


def format_annotation(annotation: Annotation) -> Table:
    table = Table.grid(padding=(0, 2))
    if annotation.out_of_scope:
        table.add_row("[bold]Out of scope[/bold]")
        return table
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
    if record.primary is not None:
        panels.append(
            Panel(
                Group(
                    format_annotation(record.primary.annotation),
                    "",
                    Text(record.primary.reasoning),
                ),
                title=f"Annotation — {record.primary_model}",
                border_style="cyan",
            )
        )
    if record.review is not None:
        verdict = "Veto" if record.review.veto else "Approval"
        panels.append(
            Panel(
                Text(record.review.reasoning),
                title=f"{verdict} — {record.review_model}",
                border_style="red" if record.review.veto else "green",
            )
        )
    if record.human is not None:
        panels.append(
            Panel(format_annotation(record.human), title="Human", border_style="blue")
        )
    return Group(*panels)


class EditScreen(ModalScreen[Annotation | None]):
    BINDINGS = [
        Binding("ctrl+s", "save", "Save"),
        Binding("escape", "cancel", "Cancel"),
    ]
    DEFAULT_CSS = """
    EditScreen { align: center middle; }
    #dialog {
        width: 90%; height: auto; max-height: 95%;
        border: thick $accent; background: $surface; padding: 1 2;
    }
    #labels { height: auto; margin-top: 1; }
    #labels > Vertical { width: 1fr; height: auto; padding-right: 1; }
    #labels Label { text-style: bold; }
    #roles { height: auto; }
    #buttons { height: auto; margin-top: 1; align-horizontal: right; }
    #buttons Button { margin-left: 1; }
    """

    def __init__(self, current: Annotation) -> None:
        super().__init__()
        self.current = current

    def compose(self) -> ComposeResult:
        current = self.current
        with Vertical(id="dialog"):
            yield Checkbox("Out of scope", current.out_of_scope, id="out-of-scope")
            with Horizontal(id="labels"):
                with Vertical():
                    yield Label("Level")
                    with RadioSet(id="level"):
                        for level in LEVELS:
                            yield RadioButton(level, value=level == current.level)
                with Vertical():
                    yield Label("Roles")
                    yield SelectionList[str](
                        *((role, role, role in current.roles) for role in ROLES),
                        id="roles",
                    )
                with Vertical():
                    yield Label("Seniority")
                    with RadioSet(id="seniority"):
                        for seniority in SENIORITIES:
                            yield RadioButton(
                                seniority, value=seniority == current.seniority
                            )
            with Horizontal(id="buttons"):
                yield Button("Save (ctrl+s)", variant="primary", id="save")
                yield Button("Cancel (esc)", id="cancel")

    def on_mount(self) -> None:
        self.set_scope(self.current.out_of_scope)

    def on_checkbox_changed(self, event: Checkbox.Changed) -> None:
        self.set_scope(event.value)

    def set_scope(self, out_of_scope: bool) -> None:
        self.query_one("#labels").disabled = out_of_scope

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "save":
            self.action_save()
        else:
            self.action_cancel()

    def action_cancel(self) -> None:
        self.dismiss(None)

    def action_save(self) -> None:
        if self.query_one("#out-of-scope", Checkbox).value:
            self.dismiss(
                Annotation(out_of_scope=True, level=None, roles=[], seniority=None)
            )
            return
        level_index = self.query_one("#level", RadioSet).pressed_index
        seniority_index = self.query_one("#seniority", RadioSet).pressed_index
        if level_index < 0 or seniority_index < 0:
            self.notify("Select a level and a seniority.", severity="error")
            return
        selected = self.query_one("#roles", SelectionList).selected
        self.dismiss(
            Annotation(
                out_of_scope=False,
                level=LEVELS[level_index],
                roles=[role for role in ROLES if role in selected],
                seniority=SENIORITIES[seniority_index],
            )
        )


class ReviewScreen(Screen[None]):
    BINDINGS = [
        Binding("1", "accept", "Accept annotation"),
        Binding("2", "edit", "Edit"),
        Binding("k", "skip", "Skip"),
        Binding("q", "app.quit", "Quit"),
    ]

    def __init__(
        self, queue: list[AnnotationRecord], on_change: Callable[[], None]
    ) -> None:
        super().__init__()
        self.queue = queue
        self.on_change = on_change
        self.position = 0

    def compose(self) -> ComposeResult:
        yield Header()
        with VerticalScroll():
            yield Static(id="record")
        yield Footer()

    def on_mount(self) -> None:
        self.show_current()

    @property
    def record(self) -> AnnotationRecord:
        return self.queue[self.position]

    def show_current(self) -> None:
        if self.position >= len(self.queue):
            self.app.exit()
            return
        self.sub_title = f"Vetoed {self.position + 1}/{len(self.queue)}"
        self.query_one("#record", Static).update(render_record(self.record))

    def decide(self, annotation: Annotation) -> None:
        self.record.human = annotation
        self.record.annotation = annotation
        self.on_change()
        self.position += 1
        self.show_current()

    def action_accept(self) -> None:
        assert self.record.primary is not None
        self.decide(self.record.primary.annotation.model_copy(deep=True))

    def action_edit(self) -> None:
        assert self.record.primary is not None
        self.app.push_screen(EditScreen(self.record.primary.annotation), self.on_edited)

    def on_edited(self, annotation: Annotation | None) -> None:
        if annotation is not None:
            self.decide(annotation)

    def action_skip(self) -> None:
        self.position += 1
        self.show_current()


class ListScreen(Screen[None]):
    BINDINGS = [Binding("q", "app.quit", "Quit")]
    DEFAULT_CSS = """
    #records { height: 1fr; }
    #detail { height: 1fr; border-top: solid $accent; }
    """

    def __init__(self, records: list[AnnotationRecord]) -> None:
        super().__init__()
        self.records = records

    def compose(self) -> ComposeResult:
        yield Header()
        yield DataTable(id="records", cursor_type="row", zebra_stripes=True)
        with VerticalScroll(id="detail"):
            yield Static(id="record")
        yield Footer()

    def on_mount(self) -> None:
        self.sub_title = f"{len(self.records)} records"
        table = self.query_one("#records", DataTable)
        table.add_columns(
            "#", "Title", "Dataset", "State", "Level", "Roles", "Seniority"
        )
        for index, record in enumerate(self.records, 1):
            state = record_state(record)
            annotation = record.annotation
            if annotation is None and record.primary is not None:
                annotation = record.primary.annotation
            if annotation is None:
                labels = ["", "", ""]
            elif annotation.out_of_scope:
                labels = ["out of scope", "", ""]
            else:
                labels = [
                    str(annotation.level),
                    ", ".join(annotation.roles),
                    str(annotation.seniority),
                ]
            table.add_row(
                str(index),
                record.item.caption[:80],
                record.item.dataset,
                Text(state, style=STATE_STYLES[state]),
                *labels,
            )

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        record = self.records[event.cursor_row]
        self.query_one("#record", Static).update(render_record(record))


class ReviewApp(App[None]):
    TITLE = "Review vetoed annotations"

    def __init__(
        self, queue: list[AnnotationRecord], on_change: Callable[[], None]
    ) -> None:
        super().__init__()
        self.queue = queue
        self.on_change = on_change

    def on_mount(self) -> None:
        self.push_screen(ReviewScreen(self.queue, self.on_change))


class ListApp(App[None]):
    TITLE = "Annotated positions"

    def __init__(self, records: list[AnnotationRecord]) -> None:
        super().__init__()
        self.records = records

    def on_mount(self) -> None:
        self.push_screen(ListScreen(self.records))
