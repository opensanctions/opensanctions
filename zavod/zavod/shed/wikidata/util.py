from functools import cache
from typing import List, Optional, Set

from nomenklatura.enrich.wikidata import WikidataEnricher
from nomenklatura.enrich.wikidata.lang import LangText
from nomenklatura.enrich.wikidata.model import Item

SPECIAL_COUNTRIES = {
    "Q15180",  # Soviet Union
    "Q237",  # Vatican city
    "Q36704",  # Yugoslavia
    "Q458",  # European Union
    "Q1246",  # Kosovo
    "Q16746854",  # Luhansk PR
    "Q16150196",  # Donetsk PR
    "Q15966495",  # Crimea Rep
    "Q174193",  # United Kingdom of Great Britain and Ireland
    "Q174193",  # United Kingdom of Great Britain and Ireland
}
LANGUAGES = ["eng", "esp", "fra", "deu", "rus", "ara"]


@cache
def _item_type_props(enricher: WikidataEnricher, qid: str) -> List[str]:
    item = enricher.fetch_item(qid)
    if item is None:
        return []
    types: List[str] = []
    for claim in item.claims:
        if claim.qualifiers.get("P582"):
            continue
        if claim.property in ("P31", "P279"):
            types.append(claim.qid)
    return types


def _item_types(enricher: WikidataEnricher, path: List[str]) -> Set[str]:
    qid = path[-1]
    types = set([qid])
    if len(path) > 6:
        return types
    for type_ in _item_type_props(enricher, qid):
        if type_ not in path:
            types.update(_item_types(enricher, path + [type_]))
    return types


@cache
def item_types(enricher: WikidataEnricher, qid: str) -> Set[str]:
    """Get all the `instance of` and `subclass of` types for an item."""
    return _item_types(enricher, [qid])


def item_labels(item: Item) -> List[LangText]:
    """Pick the labels for an item in the target languages."""
    labels: List[LangText] = []
    for lang in LANGUAGES:
        for label in item.labels:
            if label.lang == lang:
                labels.append(label)
    return labels


def item_label(item: Item) -> Optional[LangText]:
    """Pick the first-ranked label for an item."""
    labels = item_labels(item)
    if len(labels) > 0:
        return labels[0]
    return None


@cache
def is_historical_country(enricher: WikidataEnricher, qid: str) -> bool:
    types = item_types(enricher, qid)
    if "Q3024240" in types:  # historical country
        return True
    if "Q19953632" in types:  #  former administrative territorial entity
        return True
    if "Q839954" in types:  # archeological site
        return True
    return False


@cache
def is_country(enricher: WikidataEnricher, qid: str) -> bool:
    if qid in SPECIAL_COUNTRIES:
        return True
    if is_historical_country(enricher, qid):
        return False
    types = item_types(enricher, qid)
    allow = (
        "Q6256",  # country
        "Q1048835",  # political territorial entity
        "Q56061",  # administrative territorial entity
        "Q10711424",  # state with limited recognition
        "Q15239622",  # disputed territory
    )
    if len(types.intersection(allow)) > 0:
        return True
    return False


@cache
def item_countries(enricher: WikidataEnricher, item: Item) -> Set[LangText]:
    """Extract the countries linked to an item, traversing up an administrative hierarchy
    via jurisdiction/part of properties."""
    countries: Set[LangText] = set()
    if is_country(enricher, item.id):
        return [item_label(item)]

    for claim in item.claims:
        # country:
        if claim.property in ("P17", "P27"):
            if claim.qualifiers.get("P582"):
                continue
            if not is_country(enricher, claim.qid):
                continue
            text = claim.text(enricher)
            countries.add(text)
    if len(countries) > 0:
        return countries
    for claim in item.claims:
        # jurisdiction, capital of, part of:
        if claim.property in ("P1001", "P1376", "P361"):
            if claim.qualifiers.get("P582"):
                continue
            # if claim.qid in seen:
            #     continue
            subitem = enricher.fetch_item(claim.qid)
            if subitem is None:
                continue
            # print("SUBITEM", repr(subitem))
            # subseen = seen + [claim.qid]
            countries.update(item_countries(enricher, subitem))
    return countries
