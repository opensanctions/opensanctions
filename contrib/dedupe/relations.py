import sys
import itertools
from pathlib import Path
from typing import Dict, Tuple, Union, List
from nomenklatura.resolver import Identifier
from followthemoney import model
from followthemoney.schema import Schema
from followthemoney.property import Property
from followthemoney.compare import compare

from zavod.logs import get_logger
from zavod.meta import Dataset
from zavod.entity import Entity
from zavod.dedupe import get_resolver
from zavod.store import get_view, clear_store
from zavod.cli import _load_datasets

log = get_logger(__name__)
Temp = Union[None, Tuple[Property, str]]
Key = Tuple[Identifier, Identifier, Schema, Temp, Temp]


def dedupe_relations(dataset: Dataset) -> None:
    # clear_store(dataset)
    view = get_view(dataset, external=True)
    resolver = get_resolver()
    resolver.prune()
    keys: Dict[Key, List[Entity]] = {}
    for entity in view.entities():
        if (
            not entity.schema.edge
            or entity.schema.source_prop is None
            or entity.schema.target_prop is None
        ):
            continue
        sources = [Identifier.get(s) for s in entity.get(entity.schema.source_prop)]
        targets = [Identifier.get(t) for t in entity.get(entity.schema.target_prop)]
        if len(set(sources).union(targets)) > 2:
            log.warning(
                "Multi-ended edge", entity=entity.id, sources=sources, targets=targets
            )
            continue
        source = min(sources)
        target = max(targets)

        if not entity.schema.edge_directed:
            source, target = min((source, target)), max((source, target))

        key = (
            source.id,
            target.id,
            entity.schema.name,
            entity.temporal_start,
            entity.temporal_end,
        )
        if key not in keys:
            keys[key] = []
        keys[key].append(entity)

    for key, values in keys.items():
        if len(values) == 1:
            continue
        # for a, b in itertools.combinations(values, 2):
        #     score = compare(model, a, b)
        #     resolver.suggest(a.id, b.id, score=score, user="edge-dedupe")
        print("KEYS", key)
        print("ENTS", values)

    # resolver.save()


if __name__ == "__main__":
    dataset = _load_datasets([Path(p) for p in sys.argv[1:]])
    dedupe_relations(dataset)
