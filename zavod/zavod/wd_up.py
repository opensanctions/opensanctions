from collections import defaultdict
from datetime import datetime
from itertools import groupby
from languagecodes import iso_639_alpha2
from nomenklatura import Store
from nomenklatura.cache import Cache
from nomenklatura.enrich.wikidata import WD_API
from nomenklatura.enrich.wikidata.model import Claim
from nomenklatura.statement.statement import Statement
from nomenklatura.util import normalize_url, is_qid
from requests import Session
from sys import argv
from typing import Any, Dict, Generator, List, Set, Optional, cast
import json
import logging
import prefixdate
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Log, ListItem, ListView, Label, Button
from textual.screen import Screen
from textual.containers import Grid
from textual.widget import Widget
from rich.text import Text
from rich.console import RenderableType
from textual.reactive import reactive
from nomenklatura.dataset import DS
from nomenklatura.entity import CE
from nomenklatura.judgement import Judgement
from pywikibot import ItemPage, WbTime, Claim, Site
from pywikibot.data import api

from zavod.entity import Entity
from zavod import settings


log = logging.getLogger(__name__)

# quickstatements
# if we add the same property and value multiple times, each time with a different
# reference but no other differences, will they converge in one?


# can't use quickstatements for 'position held' because all qualifiers will end up
# on the same claim.
# https://github.com/everypolitician/position_statements/tree/master
# Not sure you can use the full vocabulary of quickstatements like CREATE and LAST
# using position_statements.


# def get_position_held(view, entity: EntityProxy) -> None:
#     position_occupancies = defaultdict(list)
#     wd_position_occupancies = defaultdict(list)
#
#     for person_prop, person_related in view.get_adjacent(entity):
#         if person_prop.name == "positionOccupancies":
#             occupancy = person_related
#             for occ_prop, occ_related in view.get_adjacent(person_related):
#                 if occ_prop.name == "post":
#                     position = occ_related
#                     if position.id.startswith("Q"):
#                         position_occupancies[position.id].append(occupancy)
#                         if "wd_peps" in occupancy.datasets:
#                             wd_position_occupancies[position.id].append(occupancy)
#
#     if position_occupancies and (wd_position_occupancies != position_occupancies):
#         for position_id in position_occupancies.keys():
#             occupancies = position_occupancies[position_id]
#             wd_occupancies = wd_position_occupancies.get(position_id, [])
#
#             if not occupancies:
#                 continue
#
#             wd_start_years = set()
#             wd_end_years = set()
#             for occupancy in wd_occupancies:
#                 for date in occupancy.get("startDate"):
#                     wd_start_years.add(date[:4])
#                 if len(occupancy.get("startDate")) == 0:
#                     wd_start_years.add(None)
#                 for date in occupancy.get("endDate"):
#                     wd_end_years.add(date[:4])
#                 if len(occupancy.get("endDate")) == 0:
#                     wd_end_years.add(None)
#
#             print("  ", position_id)
#             print("     Wikidata has:")
#             for occupancy in wd_occupancies:
#                 print("      ", occupancy.get("startDate"), occupancy.get("endDate"))
#
#             print("     We have:")
#             for occupancy in occupancies:
#                 start_years = {d[:4] for d in occupancy.get("startDate")}
#                 start_years.add(None) if len(occupancy.get("startDate")) == 0 else None
#                 end_years = {d[:4] for d in occupancy.get("endDate")}
#                 end_years.add(None) if len(occupancy.get("endDate")) == 0 else None
#                 if "wd_peps" in occupancy.datasets or start_years.issubset(wd_start_years) or end_years.issubset(wd_end_years):
#                     print("       skipping", occupancy.get("startDate"), occupancy.get("endDate"), occupancy.datasets)
#                 else:
#                     print("       CANDIDATE:", occupancy.get("startDate"), occupancy.get("endDate"), occupancy.datasets)


PID_DOB_TEST = "P18"
PID_REF_URL_TEST = "P93"

PROP_LABEL = {
    "P735": "given name",
    "P734": "family name",
}
SPARQL_URL = "https://query.wikidata.org/sparql"
# TODO: add support for gendered names
# TODO: When there are multiple matches, we want the one
# in the language that matches, otherwise we want the one
# in multiple languages
# https://www.wikidata.org/wiki/Q97065077 multiple languages
# https://www.wikidata.org/wiki/Q97065008 Chinese
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
    if len(names) == 0:
        return None
    for name in sorted(names):
        if "," not in name:
            return name
    return names[0]


def prefix_to_wb_time(string: str) -> WbTime:
    year = month = day = None
    prefix = prefixdate.parse(string)
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


class Action:
    pass


class CreateItemAction(Action):
    def __repr__(self):
        return "Create Wikidata item"


class SetLabelsAction(Action):
    def __init__(self, labels: Dict[str, str]):
        self.labels: Dict[str, str] = labels

    # new_item.editLabels(labels={'en': f'A funky purple item {datetime.now().isoformat()}'}, summary="Setting labels")

    def __repr__(self) -> str:
        return "Set labels %r" % self.labels


class SetDescriptionsAction(Action):
    def __init__(self, descriptions: Dict[str, str]):
        self.descriptions = descriptions

    def __repr__(self) -> str:
        return "Set descriptions %r" % self.descriptions


# item.addClaim(dateclaim, summary=u'Adding dateOfBirth')
class AddClaimAction(Action):
    def __init__(self, claim: Claim):
        self.claim = claim

    def __repr__(self) -> str:
        return "Add claim %r" % self.claim


# claim.addSources(source_claims, summary=u'Adding reference claim')
class AddSourceClaimAction(Action):
    def __init__(self, claim: Claim, source_claims: List[Claim]):
        self.claim = claim
        self.source_claim = source_claims

    def __repr__(self):
        return "Add source qualifiers %r to %r" % (self.source_claims, self.claim)


class EditSession:
    def __init__(
        self,
        cache: Cache,
        store: Store[DS, CE],
        focus_dataset: Optional[str],
    ):
        self._store = store
        self._resolver = store.resolver
        self.is_resolver_dirty = False
        self._cache = cache
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

        self.debug_file = open("debug.txt", "w")

    def _reset_entity(self):
        self.entity: Optional[Entity] = None
        self.item: Optional[ItemPage] = None
        self.item_dict: Optional[Dict[str, Any]] = None
        self.claims: Optional[List[Claim]] = None
        self.search_results: List[Dict[str, Any]] = []
        self.source_urls: Dict[str, str] = {}
        self.actions: List[Action] = []
        self.position_occupancies = defaultdict(list)
        self.position_labels = defaultdict(str)

    def get_json(self, url, params, cache_days=2):
        url = normalize_url(url, params=params)
        response = self._cache.get(url, max_age=cache_days)
        if response is None:
            log.debug("HTTP GET: %s", url)

            resp = self._http_session.get(url)
            resp.raise_for_status()
            response = resp.text
            if cache_days > 0:
                self._cache.set(url, response)
        return json.loads(response)

    def next(self):
        self._reset_entity()

        for entity in self._entities_gen:
            
            if not entity.schema.name == "Person" or not entity.target:
                continue
            if self._focus_dataset and self._focus_dataset not in entity.datasets:
                continue

            self.entity = entity
            self.qid = self.entity.id if is_qid(self.entity.id) else None
            if self.qid is None:
                self._search_items()
            else:
                self.item = self._fetch_item()

            self._get_occupancies()
            self._propose_actions()

            if self.actions or self.qid is None:
                return

    def _fetch_item(self):
        self.debug_file.write("Fetching item %s\n" % self.qid)
        self.debug_file.flush()
        self.item = ItemPage(self._wd_repo, self.qid)
        self.debug_file.write("Fetching item dict %s\n" % self.qid)
        self.debug_file.flush()
        self.item_dict = self.item.get()
        self.claims = self.item_dict["claims"]
        self.debug_file.write("Done.\n")
        self.debug_file.flush()

    def _search_items(self):
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

    def resolve(self, qid: str):
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
        self._propose_actions()
        if not self.actions:
            self.next()

    def publish(self):
        created = False
        for action in self.actions:
            if isinstance(action, CreateItemAction):
                self.item = ItemPage(self._wd_site)
                self.qid = self.item.getID()
                created = True
            elif isinstance(action, SetLabelsAction):
                self.item.editLabels(action.labels)
            elif isinstance(action, SetDescriptionsAction):
                self.item.editDescriptions(action.descriptions)
            elif isinstance(action, AddClaimAction):
                self.item.addClaim(action.claim)
            elif isinstance(action, AddSourceClaimAction):
                action.claim.addSources(action.source_claims)
            else:
                raise ValueError("Unknown action: %r" % action)
        if created:
            self.resolve(self.qid)
                

    def save_resolver(self) -> None:
        self._resolver.save()
        self.is_resolver_dirty = False

    def _propose_actions(self):
        if self.qid:
            self._check_labels()
            self._check_birthdate()
            # self._check_given_name()
            # self._check_family_name()

            # TODO: check if it has a label, other non-claim properties?
            # description
            # aliases
            # ... all the properties we care about ...
            # ... wd:everypolitition data model has hints ...

        else:
            self.actions.append(CreateItemAction())
            self._propose_labels({})

    def _check_labels(self):
        self._propose_labels(self.item_dict.get("labels", {}))

    def _propose_labels(self, exclude: Dict[str, str]) -> None:
        labels = {}
        stmts = self.entity.get_statements("name")
        for lang, group_stmts in groupby(stmts, lambda stmt: stmt.lang or "en"):
            lang_2 = iso_639_alpha2(lang)
            if lang_2 not in exclude:
                labels[lang_2] = best_label([s.value for s in group_stmts])
        if labels:
            self.actions.append(SetLabelsAction(labels))

    def _check_birthdate(self):
        pid = "P569"
        if self.claims.get(pid, []):
            return
        stmts = self.entity.get_statements("birthDate")
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
                self.debug_file.write("Couldn't provide source for {date_claim}")

    def _make_source_claims(self, stmt: Statement) -> Optional[List[Claim]]:
        source_url = self.source_urls.get(stmt.dataset)
        if not source_url:
            return None

        url_claim = Claim(self._wd_repo, "P854", is_reference=True)
        url_claim.setTarget(source_url)
        date_claim = Claim(self._wd_repo, "P813", is_reference=True)
        date_claim.setTarget(prefix_to_wb_time(stmt.last_seen[:10]))
        return [url_claim, date_claim]
    
    def _get_occupancies(self):
        for person_prop, person_related in self._view.get_adjacent(self.entity):
            if person_prop.name == "positionOccupancies":
                occupancy = person_related
                for occ_prop, occ_related in self._view.get_adjacent(person_related):
                    if occ_prop.name == "post":
                        position = occ_related
                        self.position_occupancies[position.id].append(occupancy)
                        self.position_labels[position.id] = position.get("name")

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


def render_property(entity: Entity, property: str) -> str:
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
    def session(self) -> EditSession:
        return cast(EditSession, self.app.session)

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
    result_item = reactive(None)

    def render(self):
        value = f'{self.result_item["id"]} {self.result_item["label"]}'
        description = self.result_item.get("description", None)
        if description:
            value += f"\n  {description}"
        return(Text(value))


class SearchDisplay(Widget):
    items: Dict[str, str] = reactive([])
    list_view = None

    def compose(self):
        self.list_view = ListView()
        yield self.list_view
        self.update_items()

    def watch_items(self, items: List[Dict[str, str]]):
        if self.list_view is None:
            return
        self.update_items()

    def update_items(self):
        self.list_view.clear()
        for result_item in self.items:
            search_item = SearchItem()
            search_item.result_item = result_item
            self.list_view.append(search_item)


class QuitScreen(Screen):
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


class WikidataApp(App):
    session: EditSession
    is_dirty: bool = reactive(False)

    CSS_PATH = "wd_up.tcss"
    BINDINGS = [
        ("n", "next", "Next"),
        ("p", "publish", "Publish"),
        ("r", "resolve", "Resolve"),
        ("s", "save", "Save"),
        # ("w", "exit_save", "Quit & save"),
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
        self.session.next()
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

    def action_resolve(self):
        highlighted_index = self.search_display.list_view.index
        if self.session.qid is None and highlighted_index is not None:
            highlighted_result = self.search_display.items[highlighted_index]
            qid = highlighted_result["id"]
            self.log_display.write_line(
                f"Resolving {self.session.entity.id} as {qid} {highlighted_result['label']}"
            )
            self.session.resolve(qid)
            self.session_display.refresh()
            self.search_display.items = self.session.search_results
        else:
            self.log_display.write_line("Nothing to resolve.")

    def action_publish(self):
        self.session.publish()
        self.log_display.write_line(f"Published to {self.session.qid}.")
        self.action_next()

    def action_save(self) -> None:
        self.session.save_resolver()
        self.log_display.write_line("Saved resolver changes.")

    def action_exit_hard(self) -> None:
        if self.session.is_resolver_dirty:
            self.push_screen(QuitScreen())
        else:
            self.exit(0)


def run_app(store, cache: Cache, focus_dataset: str) -> None:
    app = WikidataApp()
    app.session = EditSession(cache, store, focus_dataset)
    app.run()
