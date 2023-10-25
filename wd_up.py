from sys import argv
from typing import Dict, List, Set, Optional
from collections import defaultdict
from nomenklatura.stream import StreamEntity
from followthemoney.cli.util import path_entities
from followthemoney.proxy import EntityProxy
from zavod.meta import load_dataset_from_path
from zavod.store import get_view
from pathlib import Path
import requests
from nomenklatura.enrich.wikidata.props import PROPS_DIRECT
from nomenklatura.enrich.wikidata import WD_API
from nomenklatura.enrich.wikidata.model import Claim

# quickstatements
# if we add the same property and value multiple times, each time with a different
# reference but no other differences, will they converge in one?


def get_position_held(view, entity: EntityProxy) -> None:
    position_occupancies = defaultdict(list)
    wd_position_occupancies = defaultdict(list)

    for person_prop, person_related in view.get_adjacent(entity):
        if person_prop.name == "positionOccupancies":
            occupancy = person_related
            for occ_prop, occ_related in view.get_adjacent(person_related):
                if occ_prop.name == "post":
                    position = occ_related
                    if position.id.startswith("Q"):
                        position_occupancies[position.id].append(occupancy)
                        if "wd_peps" in occupancy.datasets:
                            wd_position_occupancies[position.id].append(occupancy)

    if position_occupancies and (wd_position_occupancies != position_occupancies):
        for position_id in position_occupancies.keys():
            occupancies = position_occupancies[position_id]
            wd_occupancies = wd_position_occupancies.get(position_id, [])

            if not occupancies:
                continue

            wd_start_years = set()
            wd_end_years = set()
            for occupancy in wd_occupancies:
                for date in occupancy.get("startDate"):
                    wd_start_years.add(date[:4])
                if len(occupancy.get("startDate")) == 0:
                    wd_start_years.add(None)
                for date in occupancy.get("endDate"):
                    wd_end_years.add(date[:4])
                if len(occupancy.get("endDate")) == 0:
                    wd_end_years.add(None)

            print("  ", position_id)
            print("     Wikidata has:")
            for occupancy in wd_occupancies:
                print("      ", occupancy.get("startDate"), occupancy.get("endDate"))

            print("     We have:")
            for occupancy in occupancies:
                start_years = {d[:4] for d in occupancy.get("startDate")}
                start_years.add(None) if len(occupancy.get("startDate")) == 0 else None
                end_years = {d[:4] for d in occupancy.get("endDate")}
                end_years.add(None) if len(occupancy.get("endDate")) == 0 else None
                if "wd_peps" in occupancy.datasets or start_years.issubset(wd_start_years) or end_years.issubset(wd_end_years):
                    print("       skipping", occupancy.get("startDate"), occupancy.get("endDate"), occupancy.datasets)
                else:
                    print("       CANDIDATE:", occupancy.get("startDate"), occupancy.get("endDate"), occupancy.datasets)
               

def iterate_entities(source_dataset: str):
    dataset = load_dataset_from_path(Path("datasets/_collections/default.yml"))
    view = get_view(dataset, external=False)

    for entity in view.entities():
        if not entity.schema.name == "Person":
            continue
        if source_dataset not in entity.datasets:
            continue
        if not entity.id.startswith("Q"):
            continue

        qid = entity.id
        print("\n--------------------")
        print(qid, entity.get("name"))

        params = { "format": "json", "ids": qid, "action": "wbgetentities"}
        r = requests.get(WD_API, params)
        r.raise_for_status()
        item = r.json().get("entities", {}).get(qid)
        claims = defaultdict(list)
        for wd_prop, claim_dicts in item.get("claims", {}).items():
            claims[wd_prop] = [Claim(c, wd_prop) for c in claim_dicts]

        # TODO: check if it has a label, other non-claim properties?

        seen_props = set()

        for wd_prop, prop in PROPS_DIRECT.items():
            if prop in {"country", "title"}:
                continue
            if prop in seen_props:
                continue
            vals = entity.get_statements(prop)
            if not vals:
                continue
            wd_vals = claims.get(wd_prop, [])
            if wd_vals:
                seen_props.add(prop)
                continue
            seen_props.add(prop)
            print(wd_prop, prop, [(v.value, v.lang, v.dataset) for v in vals], [c._value for c in wd_vals])

        


if __name__ == "__main__":
    print(argv[1])
    iterate_entities(argv[1])
    
