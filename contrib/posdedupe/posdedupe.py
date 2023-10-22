from collections import defaultdict
from sys import argv
from typing import Dict, List
from followthemoney import model
from followthemoney.compare import compare
from nomenklatura.judgement import Judgement
from followthemoney.cli.util import path_entities
from followthemoney.proxy import EntityProxy

from zavod.dedupe import get_resolver


def load_file(filename: str):
    resolver = get_resolver()
    countries: Dict[Dict[List[set]]] = defaultdict(lambda: defaultdict(set))

    for entity in path_entities(filename, EntityProxy):
        if not entity.schema.name == "Position":
            continue

        for country in entity.get("country"):
            for name in entity.get("name"):
                countries[country][name.lower()].add(entity.id)

    for country in countries.values():
        for names in country.values():
            if len(names) == 1:
                continue
            first, *rest = names
            for id in rest:
                print("merging", first, id)
                resolver.decide(first, id, Judgement.POSITIVE, user="position-dedupe")
    resolver.save()


if __name__ == "__main__":
    load_file(argv[1])
