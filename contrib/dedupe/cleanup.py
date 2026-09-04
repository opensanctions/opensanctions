from nomenklatura.resolver import Identifier
from nomenklatura.db import make_session
from followthemoney.schema import Schema
from followthemoney.property import Property

from zavod.logs import get_logger
from zavod.meta import Dataset, get_catalog
from zavod.integration import get_resolver
from zavod.runtime.manifest import Manifest
from zavod.store import get_store

log = get_logger(__name__)
Temp = tuple[Property, str] | None
Key = tuple[Identifier, Identifier, Schema, Temp, Temp]

# REMOVE = ["ca-sema-", "us-cia-", "icijol-", "trade-csl-", "eu-cor-"]


def cleanup_relations(dataset: Dataset) -> None:
    with make_session() as session:
        resolver = get_resolver(session)
        resolver.prune()
        store = get_store(Manifest.get_transient(dataset), resolver)
        store.sync()
        view = store.default_view()
        used_ids = set()
        for idx, entity in enumerate(view.entities()):
            used_ids.add(entity.id)
            used_ids.update(entity.referents)

            if idx > 0 and idx % 10000 == 0:
                log.info(f"Generated {idx} entities...")

        nodes = {
            node
            for edge in resolver.get_judgements()
            for node in (edge.source, edge.target)
        }
        unused_ids = set()
        for node in nodes:
            if node.canonical:
                continue
            if node.id in used_ids:
                continue
            resolver.remove(node)
            log.info(f"Removing: {node.id}")
            unused_ids.add(node.id)

        log.info(f"Unused IDs: {len(unused_ids)}")


if __name__ == "__main__":
    dataset = get_catalog().require("all")
    cleanup_relations(dataset)
