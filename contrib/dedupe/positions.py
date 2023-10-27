import re
import sys
from pathlib import Path
from typing import Dict, Set, Optional
from collections import defaultdict
from itertools import combinations
from fingerprints import clean_name_ascii
from nomenklatura.stream import StreamEntity
from nomenklatura.util import levenshtein_similarity

from zavod.meta import Dataset
from zavod.dedupe import get_resolver
from zavod.store import get_view, clear_store
from zavod.cli import _load_datasets

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


def crossref_positions(dataset: Dataset) -> None:
    clear_store(dataset)
    view = get_view(dataset, external=True)
    resolver = get_resolver()
    resolver.prune()
    countries: Dict[str, Dict[str, Set[StreamEntity]]] = defaultdict(
        lambda: defaultdict(set)
    )

    for entity in view.entities():
        if not entity.schema.name == "Position":
            continue

        for country in entity.get("country"):
            for name in entity.get("name"):
                name = norm_name(name)
                if name is None:
                    continue
                for token in name.split(" "):
                    countries[country][token].add(entity)
                # countries[country][name].add(entity)

    for country_posn in countries.values():
        for positions in country_posn.values():
            if len(positions) == 1:
                continue
            for left, right in combinations(positions, 2):
                score = levenshtein_similarity(left.caption, right.caption)
                resolver.suggest(left.id, right.id, score=score, user="position-dedupe")
    resolver.save()


if __name__ == "__main__":
    dataset = _load_datasets([Path(p) for p in sys.argv[1:]])
    crossref_positions(dataset)
