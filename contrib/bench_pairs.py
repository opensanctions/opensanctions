# Bench Press! ahem Pairs!
#
# What portion of current clusters are proposed by the blocker?
# Run this once with a current version to get a baseline of current recall,
# then run with changed code to see if recall has gotten worse.
#
#
#                     *umph*
#
#                        |
#        _            \    /             _
#      _|#|       .-.         .-.       |#|_
#     |#|#|______/  /_ .-'-. _\  \______|#|#|
#    [|#|#|------| (  || | ||  ) |------|#|#|]
#     |#|#|      |__|_.-'''-._|__|      |#|#|
#      "|#|                             |#|"
#        "                               "
#             A bench press. Trust me.
#
# credit: https://ascii.co.uk/art/weightlifting

from collections import defaultdict
import logging
from typing import Dict, List
from tempfile import TemporaryDirectory

import click
from followthemoney.cli.util import InPath
from pathlib import Path
from nomenklatura.resolver import Linker
from nomenklatura.resolver.identifier import Identifier

from zavod.cli import _load_datasets
from zavod.entity import Entity
from zavod.integration.dedupe import get_resolver
from zavod.integration.duckdb_index import DuckDBIndex
from zavod.logs import configure_logging
from zavod.store import get_store


@click.command()
@click.argument("dataset_paths", type=InPath, nargs=-1)
@click.option("-c", "--clear", is_flag=True, default=False)
def main(dataset_paths: List[Path], clear: bool) -> None:
    configure_logging(level=logging.INFO)

    dataset = _load_datasets(dataset_paths)
    store = get_store(dataset, Linker[Entity]({}))
    store.sync(clear=clear)
    view = store.default_view()

    resolver = get_resolver()
    resolver.begin()
    linker = resolver.get_linker()
    resolver.rollback()

    observations: Dict[str, bool] = defaultdict(bool)

    for idx, entity in enumerate(view.entities()):
        assert entity.id is not None
        for connected in linker.connected(Identifier.get(entity.id)):
            # Initialise the observation for each
            observations[connected.id] = False

        if idx > 0 and idx % 10000 == 0:
            print(f"Processed {idx} entities")

    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        index = DuckDBIndex(view, temp_path)
        index.build()
        for idx, ((left, right), _score) in enumerate(index.pairs()):
            if right in linker.connected(left):
                # if the pair is connected, observe the pair
                observations[left.id] = True
                observations[right.id] = True

            if idx > 0 and idx % 1000 == 0:
                print(f"Processed {idx} pairs")

    true_false: Dict[bool, int] = defaultdict(int)
    # Count true and false
    true_false_counted = set()
    for entity_id, is_true in observations.items():
        if entity_id in true_false_counted:
            continue
        true_false_counted.add(entity_id)
        true_false[is_true] += 1

    # Print the counts and pct true
    total = sum(true_false.values())
    print("True: %d" % true_false[True])
    print("False: %d" % true_false[False])
    print("Pct True: %.2f" % (true_false[True] / total * 100))


if __name__ == "__main__":
    main()
