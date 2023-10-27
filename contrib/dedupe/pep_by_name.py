import re
import sys
from pathlib import Path
from typing import Dict, Set, Optional
from collections import defaultdict
from itertools import combinations
from fingerprints import fingerprint
from nomenklatura.stream import StreamEntity
from nomenklatura.matching import NameQualifiedMatcher

from zavod.meta import Dataset
from zavod.dedupe import get_resolver
from zavod.store import get_view, clear_store
from zavod.cli import _load_datasets

STOPWORDS = re.compile(r"[\W](of|and|&|for|in|the|a|an|at|on|by|with|from)[\W$]", re.U)


def norm_name(name: str) -> Optional[str]:
    return fingerprint(name)


def load_file(dataset: Dataset):
    clear_store(dataset)
    view = get_view(dataset, external=True)
    resolver = get_resolver()
    resolver.prune()
    countries: Dict[str, Dict[str, Set[StreamEntity]]] = defaultdict(
        lambda: defaultdict(set)
    )

    for entity in view.entities():
        if entity.schema.name != "Person":
            continue

        for country in entity.get("country"):
            for name in entity.names:
                fp = fingerprint(name)
                if fp is None:
                    continue
                countries[country][fp].add(entity)

    for country_posn in countries.values():
        for positions in country_posn.values():
            if len(positions) == 1:
                continue
            for left, right in combinations(positions, 2):
                score = NameQualifiedMatcher.compare(left, right).score
                resolver.suggest(left.id, right.id, score=score, user="pep-dedupe")
    resolver.save()


if __name__ == "__main__":
    dataset = _load_datasets([Path(p) for p in sys.argv[1:]])
    load_file(dataset)
