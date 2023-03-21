from nomenklatura.resolver import Identifier

from opensanctions.core import Dataset, Entity
from opensanctions.core.resolver import get_resolver
from opensanctions.core.loader import Database
from followthemoney.cli.util import path_entities

# def load_db(name: str):
#     resolver = get_resolver()
#     ds = Dataset.require(name)
#     db = Database(ds, resolver, cached=False)
#     print("DB", db)
#     loader = db.view(ds)
#     for entity in loader:
#         print(entity)


def load_file(filename: str):
    keys = {}
    for entity in path_entities(filename, Entity):
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
        print(values)


if __name__ == "__main__":
    load_file("data.json")
