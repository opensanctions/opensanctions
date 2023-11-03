from collections import defaultdict
from datetime import datetime
from itertools import groupby
from languagecodes import iso_639_alpha2
from nomenklatura import Store
from nomenklatura.cache import Cache
from nomenklatura.dataset import Dataset, DS
from nomenklatura.entity import CompositeEntity, CE
from nomenklatura.judgement import Judgement
from nomenklatura.statement.statement import Statement
from nomenklatura.util import normalize_url, is_qid
from pprint import pformat
from pywikibot import ItemPage, WbTime, Claim, Site  # type: ignore
from pywikibot.data import api  # type: ignore
from requests import Session
from rich.console import RenderableType
from rich.text import Text
from sys import argv
from textual.app import App, ComposeResult
from textual.containers import Grid
from textual.reactive import reactive
from textual.screen import Screen
from textual.widget import Widget
from textual.widgets import Header, Footer, Log, ListItem, ListView, Label, Button
from textual.message import Message
from textual import work
from typing import Any, Dict, Generator, Generic, List, Set, Optional, cast
import json
import logging
import prefixdate

from zavod import settings
from zavod.entity import Entity


log = logging.getLogger(__name__)

PROP_LABEL = {
    "P735": "given name",
    "P734": "family name",
}
SPARQL_URL = "https://query.wikidata.org/sparql"
GIVEN_NAME_SPARQL = """
SELECT ?item ?itemLabel
WHERE
{
  ?item wdt:P31 wd:Q202444 .
  ?item ?label "%s"@en .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
"""
FAMILY_NAME_SPARQL = """
SELECT ?item ?itemLabel
WHERE
{
  ?item wdt:P31 wd:Q101352 .
  ?item ?label "%s"@en .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
"""


def best_label(names: List[str]) -> str:
    """Prefer labels that don't have a comma."""
    for name in sorted(names):
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
    return WbTime(year=year, month=month, day=day)


def item_dict_cache_key(qid: str) -> str:
    return f"wd:item:{qid}:dict"


class Action:
    pass


class CreateItemAction(Action):
    def __repr__(self) -> str:
        return "Create Wikidata item"


class SetLabelsAction(Action):
    def __init__(self, labels: Dict[str, str]):
        self.labels: Dict[str, str] = labels

    def __repr__(self) -> str:
        return "Set labels %r" % self.labels


class SetDescriptionsAction(Action):
    def __init__(self, descriptions: Dict[str, str]):
        self.descriptions = descriptions

    def __repr__(self) -> str:
        return "Set descriptions %r" % self.descriptions


class AddClaimAction(Action):
    def __init__(self, claim: Claim):
        self.claim = claim

    def __repr__(self) -> str:
        return "Add claim %s with value %r" % (self.claim.id, self.claim.target)


class AddSourceClaimAction(Action):
    def __init__(self, claim: Claim, source_claims: List[Claim]):
        self.claim = claim
        self.source_claims = source_claims

    def __repr__(self) -> str:
        return "Add source qualifiers %r to %r" % (self.source_claims, self.claim)


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
    the entity to the new QID.

    `save_resolver()` can be called to save the resolver state.
    """

    def __init__(
        self,
        cache: Cache,
        store: Store[DS, CE],
        focus_dataset: Optional[str],
        app: App[int],
    ):
        self._store = store
        self._resolver = store.resolver
        self.is_resolver_dirty = False
        self._cache = cache
        self._app = app
        self._view = store.default_view(external=False)
        self._focus_dataset = focus_dataset
        self._http_session = Session()
        self._http_session.headers[
            "User-Agent"
        ] = f"zavod (https://opensanctions.org; https://www.wikidata.org/wiki/User:OpenSanctions)"
        self._wd_site = Site(settings.WD_SITE_CODE, "wikidata")
        self._wd_repo = self._wd_site.data_repository()
        self._entities_gen = self._view.entities()
        self._reset_entity()

    def _reset_entity(self) -> None:
        self.entity: Optional[CE] = None
        self.item: Optional[ItemPage] = None
        self.item_dict: Optional[Dict[str, Any]] = None
        self.search_results: List[Dict[str, Any]] = []
        self.source_urls: Dict[str, str] = {}
        self.actions: List[Action] = []
        self.position_occupancies: Dict[str, List[CE]] = defaultdict(list)
        self.position_labels: Dict[str, List[str]] = {}

    def get_json(
        self, url: str, params: Dict[str, str], cache_days: Optional[int] = None
    ) -> Any:
        url = normalize_url(url, params=params)
        response = self._cache.get(url, max_age=cache_days)
        if response is None:
            log.debug("HTTP GET: %s", url)

            resp = self._http_session.get(url)
            resp.raise_for_status()
            response = resp.text
            if cache_days is not None and cache_days > 0:
                self._cache.set(url, response)
        return json.loads(response)

    def next(self) -> None:
        self._reset_entity()

        for entity in self._entities_gen:
            if not entity.schema.name == "Person" or not entity.target:
                continue
            if self._focus_dataset and self._focus_dataset not in entity.datasets:
                continue

            self.entity = entity
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

    def _fetch_item(self) -> None:
        self._log("Fetching item %s" % self.qid)
        self.item = ItemPage(self._wd_repo, self.qid)
        self._log("Fetching item dict %s\n" % self.qid)
        # dict_cache_key = item_dict_cache_key(self.qid)
        # self.item_dict = self._cache.get(dict_cache_key, max_age=1)
        # if self.item_dict is None:
        self.item_dict = self.item.get()
        if self.item_dict is None:
            raise ValueError(f"Couldn't fetch item {self.qid}")
        #    self._cache.set(dict_cache_key, self.item_dict)
        self._log("Done.\n")

    def _search_items(self) -> None:
        if self.entity is None:
            raise ValueError("No entity to search for")
        params = {
            "action": "wbsearchentities",
            "format": "json",
            "language": "en",
            "type": "item",
            "search": best_label(self.entity.get("name")),
        }
        request = api.Request(site=self._wd_site, parameters=params)
        result = request.submit()
        self.search_results = result["search"]
        for result in self.search_results:
            result["item"] = ItemPage(self._wd_repo, result["id"])
            result["item_dict"] = result["item"].get()

    def resolve(self, qid: str) -> None:
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
        # self._cache.delete(item_dict_cache_key(self.qid))
        for action in self.actions:
            if isinstance(action, CreateItemAction):
                self.item = ItemPage(self._wd_site)
                self.qid = self.item.getID()
                created = True
            elif isinstance(action, SetLabelsAction):
                if self.item is None:
                    raise ValueError("No item to publish to")
                self.item.editLabels(action.labels)
            elif isinstance(action, SetDescriptionsAction):
                if self.item is None:
                    raise ValueError("No item to publish to")
                self.item.editDescriptions(action.descriptions)
            elif isinstance(action, AddClaimAction):
                if self.item is None:
                    raise ValueError("No item to publish to")
                self.item.addClaim(action.claim)
            elif isinstance(action, AddSourceClaimAction):
                if self.item is None:
                    raise ValueError("No item to publish to")
                action.claim.addSources(action.source_claims)
            else:
                raise ValueError("Unknown action: %r" % action)
        if created:
            if self.qid is None:
                raise ValueError("No QID for created item")
            self.resolve(self.qid)

    def save_resolver(self) -> None:
        self._resolver.save()
        self.is_resolver_dirty = False

    def _propose_actions(self, entity: CE) -> None:
        if self.qid:
            if self.item_dict is None:
                raise ValueError("No item dict to propose actions for")
            self._propose_labels(entity, self.item_dict["labels"])
            self._check_birthdate(entity, self.item_dict["claims"])
            self._check_positions(self.item_dict["claims"])
            # self._check_given_name()
            # self._check_family_name()

            # TODO: check if it has a label, other non-claim properties?
            # description
            # aliases
            # ... all the properties we care about ...
            # ... wd:everypolitition data model has hints ...

        else:
            self.actions.append(CreateItemAction())
            self._propose_labels(entity, {})

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

    def _check_birthdate(
        self, entity: CE, claims: Dict[str, List[Claim]]
    ) -> None:
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
                self.actions.append(AddClaimAction(date_claim))
                self.actions.append(AddSourceClaimAction(date_claim, source_claims))
            else:
                self._log(f"Couldn't provide source for {date_claim}")

    def _make_source_claims(self, stmt: Statement) -> Optional[List[Claim]]:
        source_url = self.source_urls.get(stmt.dataset)
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
                        self.position_occupancies[position.id].append(occupancy)
                        self.position_labels[position.id] = position.get("name")

    def _check_positions(self, claims: Dict[str, List[Claim]]) -> None:
        wd_pos_start_years = defaultdict(set)
        wd_pos_end_years = defaultdict(set)
        # Wikidata
        for claim_ in claims.get("P39", []):
            claim = cast(Claim, claim_)
            self._log(str(claim.target))
            starts = [cast(Claim, q) for q in claim.qualifiers.get("P580", [])]
            ends = [cast(Claim, q) for q in claim.qualifiers.get("P582", [])]
            if len(starts) > 0:
                wd_pos_start_years[claim.target.getID()].add(str(starts[0].target.year))
            if len(ends) > 0:
                wd_pos_end_years[claim.target.getID()].add(str(ends[0].target.year))
        self._log(f"wd starts {wd_pos_start_years}")
        self._log(f"wd ends {wd_pos_end_years}")
        # OpenSanctions
        for pos_id, occs in self.position_occupancies.items():
            if not is_qid(pos_id):
                continue
            for occ in occs:
                add = True
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
                    self._log(
                        f"Would add {pos_id} {occ.get('startDate')} {occ.get('endDate')}"
                    )
                    claim = Claim(self._wd_repo, "P39")
                    claim.setTarget(ItemPage(self._wd_repo, pos_id))
                    # source_claims = self._make_source_claims(occ)

                    self.actions.append(AddClaimAction(claim))

    # def _check_given_name(self):
    #    if self.claims.get("P735", []):
    #        return
    #    for stmt in self.entity.get_statements("firstName"):
    #        self.action_for_statement("P735", GIVEN_NAME_SPARQL, stmt)

    # def _check_family_name(self):
    #    wd_claims = self.claims.get("P734", [])
    #    wd_claims.extend(self.claims.get("P1950", []))
    #    if wd_claims:
    #        return
    #    # TODO: Add support for multiple family names
    #    for stmt in self.entity.get_statements("lastName"):
    #        self.action_for_statement("P734", FAMILY_NAME_SPARQL, stmt)

    #    source_url_stmts = self.entity.get_statements("sourceUrl")
    #    for dataset, stmts in groupby(source_url_stmts, lambda stmt: stmt.dataset):
    #        values = [s.value for s in stmts]
    #        if len(values) == 1:
    #            self.source_urls[dataset] = values[0]
    #        if len(values) > 1:
    #            log.info("Multiple source URLs for dataset %s", dataset)

    # def action_for_statement(self, prop, query, stmt):
    #    if stmt.lang not in [None, "en"]:
    #        return
    #    _query = query % stmt.value
    #    r = self.session.get_json(
    #        SPARQL_URL, params={"format": "json", "query": _query}
    #    )
    #    rows = r["results"]["bindings"]
    #    if len(rows) == 1:
    #        value_qid = rows[0]["item"]["value"].split("/")[-1]


#
#        source_pairs = self.source_pairs(stmt)
#        if not source_pairs:
#            return
#        claim =
#        self.actions.append(
#            Action(
#                f"{self.qid}\t{prop}\t{value_qid}\t{source_pairs}",
#                f"Add {PROP_LABEL[prop]} {value_qid} ({stmt.value}) to {self.qid}",
#            )
#        )
#    if len(rows) > 1:
#        log.info("More than one result. Skipping. %r", rows)
#    # TODO: if len(rows) == 0: consider adding missing given names as new items.


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
                for occ in self.session.position_occupancies[pos_id]:
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
        value = f'{self.result_item["id"]} {self.result_item["label"]}'
        description = self.result_item.get("description", None)
        if description:
            value += f"\n  {description}\n"
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
        self.log_display.write_line("Press n for next entity.")

    def action_next(self) -> None:
        if self.loading_next:
            self.log_display.write_line("Already loading next entity. Ignoring.")
        else:
            self.log_display.write_line("Loading next entity...")
            self.loading_next = True
            self.load_next_entity()

    @work(thread=True)
    def load_next_entity(self) -> None:
        self.session.next()
        self.post_message(NextLoaded())

    def on_next_loaded(self, event: NextLoaded) -> None:
        self.loading_next = False
        self.session_display.refresh()
        self.search_display.items = self.session.search_results
        if self.session.qid is None:
            if self.session.search_results:
                self.log_display.write_line(
                    "Highlight a search result and [r]esolve or [p]ublish proposed wikidata item."
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
        self.session.resolve(qid)
        self.session_display.refresh()
        self.search_display.items = self.session.search_results

    def action_publish(self) -> None:
        self.session.publish()
        self.log_display.write_line(f"Published to {self.session.qid}.")
        self.action_next()

    def action_save(self) -> None:
        self.session.save_resolver()
        self.log_display.write_line("Saved resolver changes.")

    def action_exit_save(self) -> None:
        self.session.save_resolver()
        self.exit(0)

    def action_exit_hard(self) -> None:
        if self.session.is_resolver_dirty:
            self.push_screen(QuitScreen())
        else:
            self.exit(0)


def run_app(
    store: Store[DS, CE], cache: Cache, focus_dataset: Optional[str]
) -> None:
    app = WikidataApp[DS, CE]()
    app.session = EditSession[DS, CE](cache, store, focus_dataset, app)
    app.run()
