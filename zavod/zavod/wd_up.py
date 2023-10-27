from collections import defaultdict
from followthemoney.cli.util import path_entities
from followthemoney.proxy import EntityProxy
from itertools import groupby
from languagecodes import iso_639_alpha2
from nomenklatura.cache import Cache
from nomenklatura.enrich.wikidata import WD_API
from nomenklatura.enrich.wikidata.model import Claim
from nomenklatura.enrich.wikidata.props import PROPS_DIRECT
from nomenklatura.statement.statement import Statement
from nomenklatura.util import normalize_url
from requests import Session
from sys import argv
from typing import Dict, List, Set, Optional, Tuple
import json
import logging
import prefixdate

from zavod.entity import Entity
from zavod.meta import load_dataset_from_path
from zavod.runtime.cache import get_cache
from zavod.store import View, get_view


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


def best_label(stmts: List[Statement]) -> str:
    """Prefer labels that don't have a comma."""
    if len(stmts) == 0:
        return None
    for stmt in sorted(stmts, key=lambda stmt: stmt.value):
        if "," not in stmt.value:
            return stmt.value
    return stmts[0].value


def prefix_to_qs_date(string: str) -> str:
    """Convers a prefixdate to a quickstatements date."""
    prefix = prefixdate.parse(string)
    if prefix.precision == prefixdate.Precision.YEAR:
        return f"+{string}-00-00T00:00:00Z/9"
    elif prefix.precision == prefixdate.Precision.MONTH:
        return f"+{string}-00T00:00:00Z/10"
    elif prefix.precision == prefixdate.Precision.DAY:
        return f"+{string}T00:00:00Z/11"
    else:
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
        wikidataIds = entity.get("wikidataId")
        self.qid = wikidataIds[0] if wikidataIds else None

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
                label = best_label(list(group_stmts))
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
            if date:
                self.actions.append(
                    Action(
                        f"{self.qid}\tP569\t{date}",
                        f"Add birth date {stmt.value} to {self.qid}.",
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
            # TODO: consider adding reference to source dataset or sourceUrl added to entity by dataset.
            self.actions.append(
                Action(
                    f"{self.qid}\t{prop}\t{value_qid}",
                    f"Add {PROP_LABEL[prop]} {value_qid} ({stmt.value}) to {self.qid}.",
                )
            )
        if len(rows) > 1:
            log.info("More than one result. Skipping. %r", rows)
        # TODO: if len(rows) == 0: consider adding missing given names as new items.


class EditSession:
    def __init__(self, cache: Cache):
        self.cache = cache
        self.http_session = Session()
        self.http_session.headers[
            "User-Agent"
        ] = f"zavod (https://opensanctions.org; https://www.wikidata.org/wiki/User:OpenSanctions)"

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

    def assess_entity(self, entity: Entity) -> Assessment:
        assessment = Assessment(self, entity)
        assessment.generate_actions()
        return assessment


def generate_wd_statements(
    out_file: str, view: View, cache: Cache, focus_dataset: str
) -> None:
    session = EditSession(cache)

    with open(out_file, "w") as quickstatements_fh:
        for entity in view.entities():
            if not entity.schema.name == "Person":
                continue
            if focus_dataset and focus_dataset not in entity.datasets:
                continue

            assessment = session.assess_entity(entity)

            # todo: in interactive interface, offer the user the choice of adding all
            # actions, a selection, or none.
            for action in assessment.actions:
                quickstatements_fh.write(
                    f"{action.quickstatement}\t/* {action.human_readable} */\n"
                )

            quickstatements_fh.flush()
            cache.flush()
