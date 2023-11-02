import itertools
from typing import Dict, Tuple, List
from nomenklatura.resolver import Identifier
from followthemoney import model
from followthemoney.schema import Schema
from followthemoney.compare import compare
from nomenklatura.stream import StreamEntity
from followthemoney.cli.util import path_entities

from zavod.dedupe import get_resolver


def load_file(filename: str) -> None:
    resolver = get_resolver()
    keys: Dict[Tuple[Identifier, Identifier, Schema], List[StreamEntity]] = {}
    for entity in path_entities(filename, StreamEntity):
        if not entity.schema.edge:
            continue
        sources = [Identifier.get(s) for s in entity.get(entity.schema.source_prop)]
        targets = [Identifier.get(t) for t in entity.get(entity.schema.target_prop)]
        source = min(sources)
        target = max(targets)

        if not entity.schema.edge_directed:
            source, target = min((source, target)), max((source, target))

        key = (source, target, entity.schema)
        if key not in keys:
            keys[key] = []
        keys[key].append(entity)

    for key, values in keys.items():
        if len(values) == 1:
            continue
        for a, b in itertools.combinations(values, 2):
            score = compare(model, a, b)
            resolver.suggest(a.id, b.id, score=score, user="edge-dedupe")
        print(values)

    resolver.save()


if __name__ == "__main__":
    load_file("data.json")
