from functools import lru_cache
from typing import List, Optional, Set

from nomenklatura.enrich.wikidata import WikidataEnricher
from nomenklatura.enrich.wikidata.lang import LangText
from nomenklatura.enrich.wikidata.model import Item

from zavod.meta import Dataset


LANGUAGES = ["eng", "esp", "fra", "deu", "rus", "ara"]

Wikidata = WikidataEnricher[Dataset]


@lru_cache(maxsize=10000)
def _item_type_props(enricher: Wikidata, qid: str) -> List[str]:
    item = enricher.fetch_item(qid)
    if item is None:
        return []
    types: List[str] = []
    for claim in item.claims:
        # historical countries are always historical:
        ended = claim.qualifiers.get("P582") is not None and claim.qid != "Q3024240"
        if ended or claim.qid is None:
            continue
        if claim.property in ("P31", "P279"):
            types.append(claim.qid)
    return types


def _item_types(enricher: Wikidata, path: List[str]) -> Set[str]:
    qid = path[-1]
    types = set([qid])
    if len(path) > 6:
        return types
    for type_ in _item_type_props(enricher, qid):
        if type_ not in path:
            types.update(_item_types(enricher, path + [type_]))
    return types


def item_types(enricher: Wikidata, qid: str) -> Set[str]:
    """Get all the `instance of` and `subclass of` types for an item."""
    return _item_types(enricher, [qid])


def item_labels(item: Item) -> List[LangText]:
    """Pick the labels for an item in the target languages."""
    labels: List[LangText] = []
    for lang in LANGUAGES:
        for label in item.labels:
            if label.lang == lang:
                labels.append(label)
    if not len(labels):
        for label in item.labels:
            labels.append(label)
    return labels


def item_label(item: Item) -> Optional[LangText]:
    """Pick the first-ranked label for an item."""
    labels = item_labels(item)
    if len(labels) > 0:
        return labels[0]
    return None
