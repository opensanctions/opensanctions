from collections import defaultdict
from itertools import groupby
from languagecodes import iso_639_alpha2
from nomenklatura import Store
from nomenklatura.dataset import DS
from nomenklatura.entity import CE
from nomenklatura.judgement import Judgement
from nomenklatura.statement.statement import Statement
from nomenklatura.util import is_qid

# They've done a partial attempt at adding types, then totally
# deprioritised it.
from pywikibot import ItemPage, WbTime, Claim, Site  # type: ignore
from pywikibot.data import api  # type: ignore
from rich.console import RenderableType
from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Grid
from textual.reactive import reactive
from textual.screen import Screen
from textual.widget import Widget
from textual.widgets import Header, Footer, Log, ListItem, ListView, Label, Button
from textual.message import Message
from textual import work
from typing import Any, Dict, Generic, List, Optional, Tuple, cast
import logging
import prefixdate
import re

from zavod import settings
from zavod.meta.dataset import Dataset


log = logging.getLogger(__name__)

# Because you have to set it, and you have to set it to this value.
# Just grgorian results in dates being added, but the wikidata web UI being
# weird and probably disappearing your edit on the next save.
PROLEPTIC_GREGORIAN = "http://www.wikidata.org/entity/Q1985727"


def best_label(names: List[str]) -> str:
    """Prefer labels that don't have a comma."""
    multi_multi_case_names = []
    for name in names:
        if re.match("[A-Z][a-z]+ [A-Z][a-z]+", name):
            multi_multi_case_names.append(name)
    multi_names = []
    for name in names:
        if re.match("[A-Z][z-z]+", name):
            multi_names.append(name)
    for name in sorted(multi_multi_case_names) + sorted(multi_names) + sorted(names):
        if "," not in name:
            return name
    return names[0]


def prefix_to_wb_time(string: str) -> Optional[WbTime]:
    year = month = day = None
    prefix = prefixdate.parse(string)
    if prefix.dt is None:
        return None
    match prefix.precision:
        case prefixdate.Precision.YEAR:
            year = prefix.dt.year
        case prefixdate.Precision.MONTH:
            year = prefix.dt.year
            month = prefix.dt.month
        case prefixdate.Precision.DAY:
            year = prefix.dt.year
            month = prefix.dt.month
            day = prefix.dt.day
        case _:
            return None
    return WbTime(year=year, month=month, day=day, calendarmodel=PROLEPTIC_GREGORIAN)


def wd_date_to_str(date: WbTime) -> str:
    match date.precision:
        case 9:
            return str(date.year)
        case 10:
            return f"{date.year}-{date.month}"
        case 11:
            return f"{date.year}-{date.month}-{date.day}"
        case _:
            return str(date)


def wd_value_to_str(value: Any) -> str:
    if isinstance(value, ItemPage):
        return str(value.labels.get("en", value.id))
    elif isinstance(value, WbTime):
        return wd_date_to_str(value)
    else:
        return str(value)


class Action:
    pass


class CreateItemAction(Action):
    def __repr__(self) -> str:
        return "Create Wikidata item."


class SetLabelsAction(Action):
    def __init__(self, labels: Dict[str, str]):
        self.labels: Dict[str, str] = labels

    def __repr__(self) -> str:
        return "Set labels %r." % self.labels


class SetDescriptionsAction(Action):
    def __init__(self, description: str):
        self.description = description

    def __repr__(self) -> str:
        return "Set en description %r." % self.description


class AddClaimAction(Action):
    def __init__(self, claim: Claim, qualifiers: List[Claim], sources: List[Claim]):
        self.claim = claim
        self.qualifiers = qualifiers
        self.sources = sources

    def __repr__(self) -> str:
        t = self.claim.target
        value = wd_value_to_str(t)
        for qual in self.qualifiers:
            value += f", qualify {qual.getID()}: {wd_value_to_str(qual.target)}"
        for src in self.sources:
            value += f", source {src.getID()}: {wd_value_to_str(src.target)}"
        return "Add claim %s with value %r." % (self.claim.id, value)


class EditSession(Generic[DS, CE]):
    """A session for syncing a single entity with a single item on wikidata at a time.

    Call next() to iterate over entities from the selected dataset until either
    an entity without a QID is found, or an entity with a QID is found for which
    we have some proposed edits.

    If `entity` is None after calling next(), no more entities with potential for
    wikidata edits have been found.

    If `qid` is none, the session will search for matching items on wikidata and
    populate `search_results`. Either `resolve()` to indicate a matching item,
    or `publish()` to create a new item.

    `resolve()` can be called with a QID to add a deduplication
    decision for this entity and the provided QID to the resolver. After this, the
    item is fetched and proposed edits are rechecked. If no potential edits are
    found, the next entity is loaded and checked.

    `publish()` can be called to publish the proposed edits to a new or existing
    item on wikidata. If publishing created a new entity, it will also resolve
    the entity to the new QID and load the next item.

    `save_resolver()` can be called to save the resolver state.
    """

    def __init__(
        self,
        store: Store[DS, CE],
        country_code: str,
        country_adjective: str,
        focus_dataset: Optional[str],
        app: "WikidataApp[DS, CE]",
    ):
        self._store = store
        self._resolver = store.resolver
        self.is_resolver_dirty = False
        self._app = app
        self._view = store.default_view(external=False)
        self._country_code = country_code
        self._country_adjective = country_adjective
        self._focus_dataset = focus_dataset
        self._wd_site = Site(settings.WD_SITE_CODE, "wikidata")
        self._wd_repo = self._wd_site.data_repository()
        self._entities_gen = self._view.entities()
        self._reset_entity()

    def _reset_entity(self) -> None:
        self.entity: Optional[CE] = None
        self.qid: Optional[str] = None
        self.item: Optional[ItemPage] = None
        self.item_dict: Optional[Dict[str, Any]] = None
        self.search_results: List[Dict[str, Any]] = []
        self.source_urls: Dict[str, str] = {}
        self.actions: List[Action] = []
        self.position_occupancies: Dict[str, List[Tuple[CE, CE]]] = defaultdict(list)
        self.position_labels: Dict[str, List[str]] = {}

    def next(self) -> None:
        for entity in self._entities_gen:
            if self._app.quitting:
                return

            self._reset_entity()
            if not entity.schema.name == "Person" or not entity.target:
                continue
            if self._focus_dataset and self._focus_dataset not in entity.datasets:
                continue

            self.entity = entity
            self.source_urls = self._load_source_urls(entity)

            self.qid = self.entity.id if is_qid(entity.id) else None
            if self.qid is None:
                self._search_items()
            else:
                self._fetch_item()

            self._get_occupancies(entity)
            self._propose_actions(entity)

            if self.actions or self.qid is None:
                return

    def _log(self, message: str) -> None:
        self._app.post_message(LogMessage(message))

    def _warn(self, message: str) -> None:
        self._log(f"WARNING: {message}")

    def _load_source_urls(self, entity: CE) -> Dict[str, str]:
        source_urls: Dict[str, str] = {}
        source_url_stmts = entity.get_statements("sourceUrl")
        for dataset, stmts in groupby(source_url_stmts, lambda stmt: stmt.dataset):
            values = [s.value for s in stmts]
            if len(values) == 1:
                source_urls[dataset] = values[0]
            if len(values) > 1:
                log.info("Multiple source URLs for dataset %s", dataset)
        return source_urls

    def _fetch_item(self) -> None:
        self._log("Fetching item %s" % self.qid)
        self.item = ItemPage(self._wd_repo, self.qid)
        self._log("Fetching item dict %s\n" % self.qid)
        self.item_dict = self.item.get()
        if self.item_dict is None:
            raise ValueError(f"Couldn't fetch item {self.qid}")
        self._log("Done.\n")

    def _search_items(self) -> None:
        if self.entity is None:
            raise ValueError("No entity to search for")
        search_label = best_label(self.entity.get("name"))
        self._log(f"Searching item: {search_label}")
        params = {
            "action": "wbsearchentities",
            "format": "json",
            "language": "en",
            "type": "item",
            "search": search_label,
        }
        request = api.Request(site=self._wd_site, parameters=params)
        result = request.submit()
        self.search_results = result["search"]
        for result in self.search_results:
            result["item"] = ItemPage(self._wd_repo, result["id"])
            result["item_dict"] = result["item"].get()

    def resolve(self, qid: str) -> None:
        """Saves the resolve decision, then fetches the item and proposes changes,
        or loads entities until one is found with proposed changes."""
        if self.entity is None:
            self._log("No entity to resolve")
            return
        if self.entity.id is None:
            self._log("Entity has no ID. Can't resolve.")
            return
        self.qid = qid
        canonical_id = self._resolver.decide(
            self.entity.id,
            qid,
            judgement=Judgement.POSITIVE,
        )
        self._store.update(canonical_id)
        self.is_resolver_dirty = True
        self.search_results = []
        self._fetch_item()
        self.actions = []
        self._propose_actions(self.entity)
        if not self.actions:
            self.next()

    def publish(self) -> None:
        created = False
        for action in self.actions:
            if isinstance(action, CreateItemAction):
                self._log("Creating item...")
                self.item = ItemPage(self._wd_site)
                created = True
            elif isinstance(action, SetLabelsAction):
                self._log("Adding item labels...")
                if self.item is None:
                    raise ValueError("No item to publish to")
                self.item.editLabels(action.labels)
            elif isinstance(action, SetDescriptionsAction):
                self._log("Adding item descriptions...")
                if self.item is None:
                    raise ValueError("No item to publish to")
                self.item.editDescriptions({"en": action.description})
            elif isinstance(action, AddClaimAction):
                self._log("Adding claim...")
                if self.item is None:
                    raise ValueError("No item to publish to")
                self.item.addClaim(action.claim)
                for claim in action.qualifiers:
                    self._log("Adding qualifier...")
                    action.claim.addQualifier(claim)
                self._log("Adding sources...")
                action.claim.addSources(action.sources)
            else:
                raise ValueError("Unknown action: %r" % action)
        if created:
            if self.item is None:
                raise ValueError("Item not set.")
            qid = self.item.getID()
            if qid == "-1" or not isinstance(qid, str):
                raise ValueError("No QID for created item %r" % qid)
            self.resolve(qid)
        else:
            self.next()

    def save_resolver(self) -> None:
        self._resolver.save()
        self.is_resolver_dirty = False

    def _propose_actions(self, entity: CE) -> None:
        if self.qid:
            if self.item_dict is None:
                raise ValueError("No item dict to propose actions for")
            labels = self.item_dict["labels"]
            descriptions = self.item_dict["descriptions"]
            claims = self.item_dict["claims"]
        else:
            labels = {}
            descriptions = {}
            claims = {}
            self.actions.append(CreateItemAction())

        self._propose_labels(entity, labels)
        self._propose_description(entity, descriptions)
        self._propose_human(entity, claims)
        self._propose_sex_or_gender(entity, claims)
        self._propose_birthdate(entity, claims)
        self._propose_positions(claims)

    def _propose_labels(self, entity: CE, exclude: Dict[str, str]) -> None:
        labels = {}
        stmts = entity.get_statements("name")
        for lang, group_stmts in groupby(stmts, lambda stmt: stmt.lang or "en"):
            lang_2 = iso_639_alpha2(lang)
            if lang_2 is None:
                self._warn(f"no iso-639-2 code for {lang}")
                continue
            if lang_2 not in exclude:
                labels[lang_2] = best_label([s.value for s in group_stmts])
        if labels:
            self.actions.append(SetLabelsAction(labels))

    def _propose_description(self, entity: CE, descriptions: Dict[str, str]) -> None:
        if "en" in descriptions:
            return
        for items in self.position_occupancies.values():
            for pos, occ in items:
                countries = pos.get("country")
                if len(countries) != 1:
                    continue
                if countries[0] != self._country_code:
                    continue
                role = None
                if "gov.head" in pos.get("topics"):
                    role = "head of state or government"
                if "gov.executive" in pos.get("topics"):
                    role = "politician"
                if "gov.legislative" in pos.get("topics"):
                    role = "politician"
                if "gov.judicial" in pos.get("topics"):
                    role = "judge"
                if role:
                    description = f"{self._country_adjective} {role}"
                    self.actions.append(SetDescriptionsAction(description))
                    return

    def _propose_birthdate(self, entity: CE, claims: Dict[str, List[Claim]]) -> None:
        """Propose adding a date of birth if we have it and wikidata doesn't."""
        pid = "P569"
        if claims.get(pid, []):
            return
        stmts = entity.get_statements("birthDate")
        if len(stmts) == 1:
            stmt = stmts[0]
            date_value = prefix_to_wb_time(stmt.value)
            if not date_value:
                return

            date_claim = Claim(self._wd_repo, pid)
            date_claim.setTarget(date_value)

            source_claims = self._make_source_claims(stmt)

            if source_claims:
                self.actions.append(AddClaimAction(date_claim, [], source_claims))
            else:
                self._log(f"Couldn't provide source for birth date from {stmt.dataset}")

    def _propose_human(self, entity: CE, claims: Dict[str, List[Claim]]) -> None:
        pid = "P31"
        if claims.get(pid, []):
            return
        claim = Claim(self._wd_repo, pid)
        claim.setTarget(ItemPage(self._wd_repo, "Q5"))
        self.actions.append(AddClaimAction(claim, [], []))

    def _propose_sex_or_gender(
        self, entity: CE, claims: Dict[str, List[Claim]]
    ) -> None:
        pid = "P21"
        if claims.get(pid, []):
            return
        stmts = entity.get_statements("gender")
        if len(stmts) == 1:
            stmt = stmts[0]
            match stmt.value:
                case "male":
                    value = "Q6581097"
                case "female":
                    value = "Q6581072"
                case _:
                    self._log(f"Unhandled gender value {stmt.value}")
                    return
            claim = Claim(self._wd_repo, pid)
            claim.setTarget(ItemPage(self._wd_repo, value))
            source_claims = self._make_source_claims(stmt)
            if source_claims:
                self.actions.append(AddClaimAction(claim, [], source_claims))
            else:
                self._log(f"Couldn't provide source for gender from {stmt.dataset}")

    def _make_source_claims(self, stmt: Statement) -> Optional[List[Claim]]:
        if isinstance(stmt.dataset, str):
            dataset = stmt.dataset
        elif isinstance(stmt.dataset, Dataset):
            dataset = stmt.dataset.name
        source_url = self.source_urls.get(dataset)
        if not source_url:
            return None

        url_claim = Claim(self._wd_repo, "P854", is_reference=True)
        url_claim.setTarget(source_url)
        date_claim = Claim(self._wd_repo, "P813", is_reference=True)
        if stmt.last_seen is None:
            raise ValueError("No last_seen for statement %s" % stmt.canonical_id)
        date_claim.setTarget(prefix_to_wb_time(stmt.last_seen[:10]))
        return [url_claim, date_claim]

    def _get_occupancies(self, entity: CE) -> None:
        for person_prop, person_related in self._view.get_adjacent(entity):
            if person_prop.name == "positionOccupancies":
                occupancy = person_related
                for occ_prop, occ_related in self._view.get_adjacent(person_related):
                    if occ_prop.name == "post":
                        position = occ_related
                        assert isinstance(position.id, str)
                        self.position_occupancies[position.id].append(
                            (position, occupancy)
                        )
                        self.position_labels[position.id] = position.get("name")

    def _propose_positions(self, claims: Dict[str, List[Claim]]) -> None:
        """Propose adding positions for any positions with QID which the wikidata
        item does not yet have for either the same start or end year"""
        wd_pos_start_years = defaultdict(set)
        wd_pos_end_years = defaultdict(set)
        unqualified_pos_ids = set()
        # Wikidata
        for claim_ in claims.get("P39", []):
            claim = cast(Claim, claim_)
            starts = [cast(Claim, q) for q in claim.qualifiers.get("P580", [])]
            ends = [cast(Claim, q) for q in claim.qualifiers.get("P582", [])]
            if len(starts) > 0:
                wd_pos_start_years[claim.target.getID()].add(str(starts[0].target.year))
            if len(ends) > 0:
                wd_pos_end_years[claim.target.getID()].add(str(ends[0].target.year))
            if not starts and not ends:
                unqualified_pos_ids.add(claim.target.getID())
        # OpenSanctions
        for pos_id, occs in self.position_occupancies.items():
            if not is_qid(pos_id):
                continue
            for pos, occ in occs:
                add = True
                if pos_id in unqualified_pos_ids:
                    add = False
                if pos_id in wd_pos_start_years:
                    for date in occ.get("startDate"):
                        if date[:4] in wd_pos_start_years[pos_id]:
                            add = False
                if pos_id in wd_pos_start_years:
                    for date in occ.get("endDate"):
                        if date[:4] in wd_pos_end_years[pos_id]:
                            add = False
                if (
                    not occ.get("startDate")
                    and not occ.get("endDate")
                    and pos_id in wd_pos_start_years
                    and pos_id in wd_pos_start_years
                ):
                    add = False
                if add:
                    claim = Claim(self._wd_repo, "P39")
                    claim.setTarget(ItemPage(self._wd_repo, pos_id))
                    source_claims = self._make_source_claims(
                        occ.get_statements("holder")[0]
                    )

                    qualifiers = []
                    start_date = occ.get("startDate")
                    if start_date:
                        start_qual = Claim(self._wd_repo, "P580", is_qualifier=True)
                        start_qual.setTarget(prefix_to_wb_time(start_date[0]))
                        qualifiers.append(start_qual)
                        source_claims_start = self._make_source_claims(
                            occ.get_statements("startDate")[0]
                        )
                        assert source_claims_start == source_claims

                    end_date = occ.get("endDate")
                    if end_date:
                        end_qual = Claim(self._wd_repo, "P582", is_qualifier=True)
                        end_qual.setTarget(prefix_to_wb_time(end_date[0]))
                        qualifiers.append(end_qual)
                        source_claims_end = self._make_source_claims(
                            occ.get_statements("endDate")[0]
                        )
                        assert source_claims_end == source_claims

                    if source_claims:
                        self.actions.append(
                            AddClaimAction(claim, qualifiers, source_claims)
                        )
                    else:
                        self._log(
                            (
                                "Couldn't provide source for position "
                                f"{pos_id} {start_date} {end_date} from {occ.dataset}"
                            )
                        )


def render_property(entity: CE, property: str) -> str:
    text = f"{property}:"
    values = entity.get(property)
    match len(values):
        case 0:
            return text + " -\n"
        case 1:
            return text + f" {values[0]}\n"
        case _:
            text += "\n"
            for value in values:
                text += f"  {value}\n"
            return text


class SessionDisplay(Widget):
    @property
    def session(self) -> EditSession[DS, CE]:
        return cast("WikidataApp[DS, CE]", self.app).session

    def render(self) -> RenderableType:
        if self.session.entity:
            text = (
                f"ID: {self.session.entity.id}\n"
                f"Names: {' | '.join(self.session.entity.get('name'))}\n\n"
            )
            text += render_property(self.session.entity, "birthDate")
            text += render_property(self.session.entity, "gender")
            text += render_property(self.session.entity, "nationality")
            text += render_property(self.session.entity, "country")
            text += "Positions:\n"
            for pos_id, pos_names in self.session.position_labels.items():
                text += f"  {pos_id}\n  {pos_names[0]} ({len(pos_names)})\n"
                for pos, occ in self.session.position_occupancies[pos_id]:
                    text += f'    {occ.get("startDate")} {occ.get("endDate")}\n'
            text += "\nProposed actions:\n"
            for action in self.session.actions:
                text += f"  {action}\n"
            return Text(text)
        else:
            return Text("No current entity")


class SearchItem(ListItem):
    result_item: reactive[Optional[Dict[str, Any]]] = reactive(None)

    def render(self) -> Text:
        if self.result_item is None:
            return Text("Result not loaded yet.")
        value = f'{self.result_item["id"]} {self.result_item["label"]}\n'
        description = self.result_item.get("description", None)
        if description:
            value += f"  {description}\n"
        value += "instance of: "
        value += ",".join(
            str(c.target)
            for c in self.result_item["item_dict"]["claims"].get("P31", [])
        )
        value += "\npositions held: \n"
        pos_claims = self.result_item["item_dict"]["claims"].get("P39", [])
        for claim in pos_claims:
            claim_ = cast(Claim, claim)
            value += f"  {claim_.target}\n"

        return Text(value)


class SearchDisplay(Widget):
    items: reactive[List[Dict[str, str]]] = reactive([])
    list_view: Optional[ListView] = None

    def compose(self) -> ComposeResult:
        self.list_view = ListView()
        yield self.list_view
        self.update_items()

    def watch_items(self, items: List[Dict[str, str]]) -> None:
        if self.list_view is None:
            return
        self.update_items()

    def update_items(self) -> None:
        if self.list_view is None:
            raise ValueError("This shouldn't happen: No list view")
        self.list_view.clear()
        for result_item in self.items:
            search_item = SearchItem()
            search_item.result_item = result_item
            self.list_view.append(search_item)


class QuitScreen(Screen[None]):
    """Screen with a dialog to quit."""

    def compose(self) -> ComposeResult:
        yield Grid(
            Label("You have unsaved changes. Quit without saving?", id="question"),
            Button("Quit", variant="error", id="quit"),
            Button("Cancel", variant="primary", id="cancel"),
            id="dialog",
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "quit":
            self.app.exit(0)
        else:
            self.app.pop_screen()


class NextLoaded(Message):
    pass


class LogMessage(Message):
    def __init__(self, message: str):
        super().__init__()
        self.message = message


class WikidataApp(App[int], Generic[DS, CE]):
    quitting: bool = False
    session: EditSession[DS, CE]
    is_dirty: reactive[bool] = reactive(False)
    loading_next: bool = False  # very very poor man's semaphore

    CSS_PATH = "wd_up.tcss"
    BINDINGS = [
        ("n", "next", "Next"),
        ("p", "publish", "Publish"),
        ("r", "resolve", "Resolve"),
        ("s", "save", "Save"),
        ("w", "exit_save", "Quit & save"),
        ("q", "exit_hard", "Quit"),
    ]

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header()
        yield Footer()
        self.session_display = SessionDisplay(classes="box")
        yield self.session_display
        self.search_display = SearchDisplay(classes="box")
        yield self.search_display
        self.log_display = Log()
        yield self.log_display
        self.action_next()

    def action_next(self) -> None:
        if self.loading_next:
            self.log_display.write_line("Already loading next entity. Ignoring.")
        else:
            self.log_display.write_line("Loading next entity...")
            self.loading_next = True
            self.do_next()

    @work(thread=True)
    def do_next(self) -> None:
        self.session.next()
        self.post_message(NextLoaded())

    def on_next_loaded(self, event: NextLoaded) -> None:
        self.loading_next = False
        self.session_display.refresh()
        self.search_display.items = self.session.search_results
        if self.session.entity is None:
            self.log_display.write_line("No more entities to edit.")
            return
        if self.session.qid is None:
            if self.session.search_results:
                self.log_display.write_line(
                    (
                        "Highlight a search result and [r]esolve "
                        "or [p]ublish proposed wikidata item."
                    )
                )
            else:
                self.log_display.write_line(
                    "No results found. [p]ublish proposed wikidata item?"
                )
        else:
            self.log_display.write_line(
                "[p]ublish proposed edits to found wikidata item?"
            )

    def on_log_message(self, event: LogMessage) -> None:
        self.log_display.write_line(event.message)

    def action_resolve(self) -> None:
        if self.loading_next:
            self.log_display.write_line("Busy working on entity. Ignoring.")
            return

        highlighted_index: Optional[int] = None
        if self.search_display.list_view:
            highlighted_index = self.search_display.list_view.index
        if (
            highlighted_index is None
            or self.session.qid is not None
            or self.session.entity is None
        ):
            self.log_display.write_line("Nothing to resolve.")
            return

        highlighted_result = self.search_display.items[highlighted_index]
        qid = highlighted_result["id"]
        self.log_display.write_line(
            f"Resolving {self.session.entity.id} as {qid} {highlighted_result['label']}"
        )
        self.loading_next = True
        self.do_resolve(qid)

    @work(thread=True)
    def do_resolve(self, qid: str) -> None:
        self.session.resolve(qid)
        self.post_message(NextLoaded())

    def action_publish(self) -> None:
        if self.loading_next:
            self.log_display.write_line("Busy working on entity. Ignoring.")
            return

        self.log_display.write_line("Publishing...")
        self.log_display.write_line(
            "Reminder: wikidata API can throttle with 5-10s wait."
        )
        self.do_publish()

    @work(thread=True)
    def do_publish(self) -> None:
        self.session.publish()
        self.post_message(NextLoaded())

    def action_save(self) -> None:
        self.session.save_resolver()
        self.log_display.write_line("Saved resolver changes.")

    def action_exit_save(self) -> None:
        self.session.save_resolver()
        self.quitting = True
        self.exit(0)

    def action_exit_hard(self) -> None:
        if self.session.is_resolver_dirty:
            self.push_screen(QuitScreen())
        else:
            self.quitting = True
            self.exit(0)


def run_app(
    store: Store[DS, CE],
    country_code: str,
    country_adjective: str,
    focus_dataset: Optional[str],
) -> None:
    app = WikidataApp[DS, CE]()
    app.session = EditSession[DS, CE](
        store, country_code, country_adjective, focus_dataset, app
    )
    app.run()
