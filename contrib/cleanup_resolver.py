import sys
from nomenklatura.stream import StreamEntity
from followthemoney.cli.util import path_entities

from zavod.dedupe import get_resolver

REMOVE = ["ca-sema-", "us-cia-", "icijol-", "trade-csl-", "eu-cor-"]


def load_file(filename: str):
    used_ids = set()
    for entity in path_entities(filename, StreamEntity):
        used_ids.add(entity.id)
        used_ids.update(entity.referents)

    print("Used IDs", len(used_ids))

    resolver = get_resolver()
    unused_ids = set()
    for node in resolver.nodes.keys():
        if node.id not in used_ids:
            if node.id.startswith("Q") or node.id.startswith("evpo-"):
                continue
            for rem in REMOVE:
                if node.id.startswith(rem):
                    resolver.remove(node)
                    print("Removing", node)
                    break
            unused_ids.add(node.id)
            print("Unused", node)
    print("Unused IDs", len(unused_ids))
    # resolver.save()


if __name__ == "__main__":
    load_file(sys.argv[1])
