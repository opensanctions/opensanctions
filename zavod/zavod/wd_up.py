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
from typing import Dict, Generator, List, Set, Optional, cast
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
import pywikibot

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


def prefix_to_qs_date(string: str) -> str:
    """Convers a prefixdate to a quickstatements date."""
    prefix = prefixdate.parse(string)
    match prefix.precision:
        case prefixdate.Precision.YEAR:
            return f"+{string}-00-00T00:00:00Z/9"
        case prefixdate.Precision.MONTH:
            return f"+{string}-00T00:00:00Z/10"
        case prefixdate.Precision.DAY:
            return f"+{string}T00:00:00Z/11"
        case _:
            log.warning("Unhandled prefixdate precision: %s", string)


class Action:
    def __init__(self, quickstatement, human_readable):
        self.quickstatement = quickstatement
        self.human_readable = human_readable


class Assessment:
    def __init__(self, session: "EditSession", entity: Entity):
        self.session = session
        self.entity = entity
        self.item = None
        self.claims = defaultdict(list)
        self.actions = []
        self.qid = entity.id if is_qid(entity.id) else None
        self.source_urls = {}

        source_url_stmts = self.entity.get_statements("sourceUrl")
        for dataset, stmts in groupby(source_url_stmts, lambda stmt: stmt.dataset):
            values = [s.value for s in stmts]
            if len(values) == 1:
                self.source_urls[dataset] = values[0]
            if len(values) > 1:
                log.info("Multiple source URLs for dataset %s", dataset)

    def fetch_item(self):
        params = {"format": "json", "ids": self.qid, "action": "wbgetentities"}
        self.item = (
            self.session.get_json(WD_API, params).get("entities", {}).get(self.qid)
        )
        if self.item is None:
            log.warning("No item for %s", self.qid)
            return
        self.claims = defaultdict(list)
        for wd_prop, claim_dicts in self.item.get("claims", {}).items():
            self.claims[wd_prop] = [Claim(c, wd_prop) for c in claim_dicts]

    def generate_actions(self):
        if self.qid:
            if self.item is None:
                self.fetch_item()
            if self.item is None:
                return
            self.assess_labels()
            self.assess_birthdate()
            self.assess_given_name()
            self.assess_family_name()

            # TODO: check if it has a label, other non-claim properties?
            # description
            # aliases
            # ... all the properties we care about ...
            # ... wd:everypolitition data model has hints ...

        # else: search for and propose adding a new item

    def assess_labels(self):
        stmts = self.entity.get_statements("name")
        for lang, group_stmts in groupby(stmts, lambda stmt: stmt.lang or "en"):
            lang_2 = iso_639_alpha2(lang)
            if lang_2 not in self.item.get("labels"):
                label = best_label([s.value for s in group_stmts])
                self.actions.append(
                    Action(
                        f'{self.qid}\tL{lang_2}\t"{label}"',
                        f"Add {lang_2} label {label} to {self.qid}.",
                    )
                )

    def assess_birthdate(self):
        if self.claims.get("P569", []):
            return
        stmts = self.entity.get_statements("birthDate")
        if len(stmts) == 1:
            stmt = stmts[0]
            date = prefix_to_qs_date(stmt.value)
            if not date:
                return
            source_pairs = self.source_pairs(stmt)
            if not source_pairs:
                return
            self.actions.append(
                Action(
                    f"{self.qid}\tP569\t{date}\t{source_pairs}",
                    f"Add birth date {stmt.value} to {self.qid}",
                )
            )

    def assess_given_name(self):
        if self.claims.get("P735", []):
            return
        for stmt in self.entity.get_statements("firstName"):
            self.action_for_statement("P735", GIVEN_NAME_SPARQL, stmt)

    def assess_family_name(self):
        wd_vals = self.claims.get("P734", [])
        wd_vals.extend(self.claims.get("P1950", []))
        if wd_vals:
            return
        # TODO: Add support for multiple family names
        for stmt in self.entity.get_statements("lastName"):
            self.action_for_statement("P734", FAMILY_NAME_SPARQL, stmt)

    def action_for_statement(self, prop, query, stmt):
        if stmt.lang not in [None, "en"]:
            return
        _query = query % stmt.value
        r = self.session.get_json(
            SPARQL_URL, params={"format": "json", "query": _query}
        )
        rows = r["results"]["bindings"]
        if len(rows) == 1:
            value_qid = rows[0]["item"]["value"].split("/")[-1]

            source_pairs = self.source_pairs(stmt)
            if not source_pairs:
                return
            self.actions.append(
                Action(
                    f"{self.qid}\t{prop}\t{value_qid}\t{source_pairs}",
                    f"Add {PROP_LABEL[prop]} {value_qid} ({stmt.value}) to {self.qid}",
                )
            )
        if len(rows) > 1:
            log.info("More than one result. Skipping. %r", rows)
        # TODO: if len(rows) == 0: consider adding missing given names as new items.

    def source_pairs(self, stmt: Statement) -> Optional[str]:
        source_url = self.source_urls.get(stmt.dataset)
        if not source_url:
            log.warning(
                "No source URL for %s birth date %s %s",
                self.qid,
                stmt.value,
                stmt.dataset,
            )
            return None
        return f'S854\t"{source_url}"\tS813\t{prefix_to_qs_date(stmt.last_seen[:10])}'


class EditSession:
    def __init__(
        self,
        cache: Cache,
        store: Store[DS, CE],
        focus_dataset: Optional[str],
        qs_filename: str,
    ):
        self.store = store
        self.resolver = store.resolver
        self.cache = cache
        self.view = store.default_view(external=False)
        self.assessment = None
        self.focus_dataset = focus_dataset
        self.http_session = Session()
        self.http_session.headers[
            "User-Agent"
        ] = f"zavod (https://opensanctions.org; https://www.wikidata.org/wiki/User:OpenSanctions)"
        self.quickstatements_fh = open(qs_filename, "w")
        self._entities_gen = self.view.entities()
        self.search_results = []

    def get_json(self, url, params, cache_days=2):
        url = normalize_url(url, params=params)
        response = self.cache.get(url, max_age=cache_days)
        if response is None:
            log.debug("HTTP GET: %s", url)

            resp = self.http_session.get(url)
            resp.raise_for_status()
            response = resp.text
            if cache_days > 0:
                self.cache.set(url, response)
        return json.loads(response)

    def next(self):
        # for action in assessment.actions:
        #     quickstatements_fh.write(
        #         f"{action.quickstatement}\t/* {action.human_readable} */\n"
        #     )
        # quickstatements_fh.flush()
        self.search_results = []
        for entity in self._entities_gen:
            if not entity.schema.name == "Person":
                continue
            if self.focus_dataset and self.focus_dataset not in entity.datasets:
                continue

            self.assessment = self.assess_entity(entity)
            if self.assessment.qid is None:
                self.search_items()
            self.cache.flush()
            if self.assessment.actions or self.assessment.qid is None:
                return

    def assess_entity(self, entity: Entity) -> Assessment:
        assessment = Assessment(self, entity)
        assessment.generate_actions()
        return assessment

    def search_items(self):
        params = {
            "format": "json",
            "search": best_label(self.assessment.entity.get("name")),
            "action": "wbsearchentities",
            "language": "en",
        }
        self.search_results = self.get_json(WD_API, params)["search"]

    # Create a decision that marks the current item id and the provided
    # qid as a positive match.
    def resolve(self, qid: str):
        canonical_id = self.resolver.decide(
            self.assessment.entity.id,
            qid,
            judgement=Judgement.POSITIVE,
        )
        self.store.update(canonical_id)

    def save(self) -> None:
        self.resolver.save()


class SessionDisplay(Widget):
    @property
    def session(self) -> EditSession:
        return cast(EditSession, self.app.session)

    # renders a table of the key biographical properties of the current assessment entity
    # and a list of proposed actions using the Rich library, if there's a current assessment in the session,
    # otherwise the message "No more entities".
    def render(self) -> RenderableType:
        if self.session.assessment:
            assessment = self.session.assessment
            return Text(
                f"{assessment.entity.id} {assessment.qid}"
                + f"\n{assessment.entity.caption}\n\n"
                + "\n".join([action.human_readable for action in assessment.actions])
            )
        else:
            return Text("No more entities")


class SearchItem(ListItem):
    result_item = reactive(None)

    def render(self):
        return f'{self.result_item["id"]} {self.result_item["label"]}'


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
        self.list_view.append(ListItem(Label("None of the above")))



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
        ("r", "resolve", "Resolve"),
        ("s", "save", "Save"),
        # ("w", "exit_save", "Quit & save"),
        ("q", "exit_hard", "Quit"),
    ]

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header()
        yield Footer()
        self.session.next()
        self.session_display = SessionDisplay(classes="box")
        yield self.session_display
        self.search_display = SearchDisplay(classes="box")
        yield self.search_display
        self.log_display = Log()
        yield self.log_display
        self.search_display.items = self.session.search_results

    def action_next(self) -> None:
        self.log_display.write_line("Loading next entity...")
        self.session.next()
        self.log_display.write_line("Done.")
        self.session_display.refresh()
        self.search_display.items = self.session.search_results

    def action_resolve(self):
        self.is_dirty = True
        highlighted_index = self.search_display.list_view.index
        highlighted_result = self.search_display.items[highlighted_index]
        qid = highlighted_result["id"]
        entity = self.session.assessment.entity
        self.log_display.write_line(
            f"Resolving {entity.id} as {qid} {highlighted_result['label']}"
        )
        self.session.resolve(qid)
        self.action_next()

    def action_save(self) -> None:
        self.session.save()
        self.is_dirty = False

    def action_exit_hard(self) -> None:
        if self.is_dirty:
            self.push_screen(QuitScreen())
        else:
            self.exit(0)


PID_DOB_TEST = "P18"

def run_app(out_file: str, store, cache: Cache, focus_dataset: str) -> None:

    site = pywikibot.Site("test", "wikidata")
    
    repo = site.data_repository()
    item = pywikibot.ItemPage(repo, "Q232548")
    print(item)
    print()
    item_dict = item.get() #Get the item dictionary
    clm_dict = item_dict["claims"] # Get the claim dictionary
    print(clm_dict)

    print()
    clm_list = clm_dict["P18"]

    for clm in clm_list:
        print(clm)
    print()

    new_item = pywikibot.ItemPage(site)
    new_item.editLabels(labels={'en': f'A funky purple item {datetime.now().isoformat()}'}, summary="Setting labels")
    qid = new_item.getID()
    print("New qid", qid)

    dateclaim = pywikibot.Claim(repo, PID_DOB_TEST)
    dateOfBirth = pywikibot.WbTime(year=1977, month=6, day=8)
    dateclaim.setTarget(dateOfBirth)
    item.addClaim(dateclaim, summary=u'Adding dateOfBirth')
    #app = WikidataApp()
    #app.session = EditSession(cache, store, focus_dataset, out_file)
    #app.run()

