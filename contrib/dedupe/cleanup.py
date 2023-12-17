from typing import Tuple, Union
from nomenklatura.resolver import Identifier
from followthemoney.schema import Schema
from followthemoney.property import Property

from zavod.logs import get_logger
from zavod.meta import Dataset, get_catalog
from zavod.dedupe import get_resolver
from zavod.store import get_view, clear_store

log = get_logger(__name__)
Temp = Union[None, Tuple[Property, str]]
Key = Tuple[Identifier, Identifier, Schema, Temp, Temp]

# REMOVE = ["ca-sema-", "us-cia-", "icijol-", "trade-csl-", "eu-cor-"]


def cleanup_relations(dataset: Dataset) -> None:
    clear_store(dataset)
    view = get_view(dataset, external=True)
    used_ids = set()
    for idx, entity in enumerate(view.entities()):
        used_ids.add(entity.id)
        used_ids.update(entity.referents)

        if idx > 0 and idx % 10000 == 0:
            log.info("Generated %s entities..." % idx)

    resolver = get_resolver()
    resolver.prune()
    unused_ids = set()
    for node in resolver.nodes.keys():
        if node.canonical:
            continue
        if node.id in used_ids:
            continue
        resolver.remove(node)
        log.info("Removing: %s" % node.id)
        unused_ids.add(node.id)

    log.info("Unused IDs: %s" % len(unused_ids))
    resolver.save()


if __name__ == "__main__":
    dataset = get_catalog().require("all")
    cleanup_relations(dataset)
