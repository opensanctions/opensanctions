from collections import defaultdict

from zavod.context import Context
from zavod.meta import Dataset
from zavod.store import View


def summarise_data(context, view):
    for entity in view.entities():
        if entity.schema.name == "Person":
            # Gather data about a person
            pos_occ = defaultdict(list)
            for person_prop, person_related in view.get_adjacent(entity):
                if person_prop.name == "positionOccupancies":
                    for occ_prop, occ_related in view.get_adjacent(person_related):
                        if occ_prop.name == "post":
                            pos_occ[(occ_related.id, str(occ_related.get("name")))].append(person_related)
            # Print data about a person
            print()
            print(", ".join(entity.get("name")))
            for ((id, name), occupancies) in pos_occ.items():
                print(f"  {id} {name}")
                for occ in occupancies:
                    print(f'    status={occ.get("status")} from={occ.get("startDate")} until={occ.get("endDate")}')


def print_peps_summary(dataset: Dataset, view: View) -> None:
    """Dump the contents of the dataset to the output directory."""
    try:
        context = Context(dataset)
        context.begin(clear=False)
        summarise_data(context, view)

    finally:
        context.close()
