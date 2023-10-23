import re
from sys import argv
from typing import Dict, Set, Optional
from collections import defaultdict
from itertools import combinations
from fingerprints import clean_name_ascii
from nomenklatura.stream import StreamEntity
from nomenklatura.util import levenshtein_similarity
from followthemoney.cli.util import path_entities

from zavod.dedupe import get_resolver

STOPWORDS = re.compile(r"[\W](of|and|&|for|in|the|a|an|at|on|by|with|from)[\W$]", re.U)


def norm_name(name: str) -> Optional[str]:
    # name = name.lower()
    cleaned = clean_name_ascii(name)
    if cleaned is None:
        return None
    while True:
        cleaned_sub = STOPWORDS.sub(" ", cleaned)
        if cleaned_sub == cleaned:
            break
        cleaned = cleaned_sub
    return cleaned


def load_file(filename: str):
    resolver = get_resolver()
    resolver.prune()
    countries: Dict[str, Dict[str, Set[StreamEntity]]] = defaultdict(
        lambda: defaultdict(set)
    )

    for entity in path_entities(filename, StreamEntity):
        if not entity.schema.name == "Position":
            continue

        for country in entity.get("country"):
            for name in entity.get("name"):
                name = norm_name(name)
                if name is None:
                    continue
                countries[country][name].add(entity)

    for country_posn in countries.values():
        for positions in country_posn.values():
            if len(positions) == 1:
                continue
            for left, right in combinations(positions, 2):
                score = levenshtein_similarity(left.caption, right.caption)
                resolver.suggest(left.id, right.id, score=score, user="position-dedupe")
    resolver.save()


if __name__ == "__main__":
    load_file(argv[1])
