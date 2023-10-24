from sys import argv
from typing import Dict, List, Set, Optional
from collections import defaultdict
from nomenklatura.stream import StreamEntity
from followthemoney.cli.util import path_entities
from followthemoney.proxy import EntityProxy
from zavod.meta import load_dataset_from_path
from zavod.store import get_view
from pathlib import Path

# 
# def is_more_specific_date(a: Optional[str], b: Optional[str]) -> bool:
#     if a is None:
#         return False
#     if b is None:
#         return True
#     if len(a) > len(b):
#         return True
#     if len(a) < len(b):
#         return False
#     
# 
# def is_more_specific_entity_date(a: EntityProxy, b: EntityProxy) -> bool:
#     a_start_date = a.get("startDate")
#     a_end_date = a.get("endDate")
#     b_start_date = b.get("startDate")
#     b_end_date = b.get("endDate")
#     if a_end_date 
#         return False
#     
# 
# # A function that takes a list of occupancies indicating a person holding a position,
# # deduplicates instances of a person holding a position from different dataset sources,
# # and returns the deduplicated list of occupancies, keeping the ones with the most specific
# # dates. Dates are partial ISO-8601 dates. If there are two occupancies from the same year but
# # one has a month, keep the one with month. If one has a day number, keep that one.
# # If the dates are the same, it keeps the one from wd_peps.
# def deduplicate_occupancies(occupancies: List[EntityProxy]) -> List[EntityProxy]:
#     date_pairs = {}
#     for occupancy in occupancies:
#         for length in [10, 7, 4, 0]:
#             start_date = occupancy.get("startDate")
#             end_date = occupancy.get("endDate")
#             if start_date is not None:
#                 start_date = start_date[:length]
#             if end_date is not None:
#                 end_date = end_date[:length]
#             match = date_pairs.get((start_date, end_date), None)
#             if match.id == occupancy.id:
#                 continue
#             if is_more_specific(occupancy, match):
#                 date_pairs[(start_date, end_date)] = occupancy
#         





def load_file():
    dataset = load_dataset_from_path(Path("datasets/_collections/peps.yml"))
    view = get_view(dataset, external=False)

    for entity in view.entities():
        if not entity.schema.name == "Person":
            continue
        if not entity.id.startswith("Q"):
            continue

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
            print("\n--------------------")
            print(entity.id, entity.get("name"))

            for position_id in position_occupancies.keys():
                print("  ", position_id)
                occupancies = position_occupancies[position_id]
                wd_occupancies = wd_position_occupancies.get(position_id, [])

                print("  Wikidata has:")
                for occupancy in wd_occupancies:
                    print("    ", occupancy.get("startDate"), occupancy.get("endDate"))

                print("  We have:")
                for occupancy in occupancies:
                    if "wd_peps" in occupancy.datasets:
                        continue
                    print("    ", occupancy.get("startDate"), occupancy.get("endDate"), occupancy.datasets)
           


if __name__ == "__main__":
    load_file()
