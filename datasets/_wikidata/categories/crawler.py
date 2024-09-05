import csv
from io import StringIO
from datetime import timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
from nomenklatura.enrich.wikidata import WikidataEnricher
from nomenklatura.enrich.wikidata.model import Item

from zavod import Context, Entity
from zavod import helpers as h

LANGUAGES = ["eng", "esp", "fra", "deu", "rus"]
URL = "https://petscan.wmcloud.org/"
QUERY = {
    "doit": "",
    "depth": 4,
    # "combination": "subset",
    "format": "csv",
    "wikidata_item": "with",
    "wikidata_prop_item_use": "Q5",
    "search_max_results": 1000,
    "sortorder": "ascending",
}


def title_name(title: str) -> str:
    return title.replace("_", " ")


def check_item_relevant(
    context: Context, enricher: WikidataEnricher, item: Item
) -> bool:
    if not item.is_instance("Q5"):
        # context.log.warning("Not a person", qid=item.id)
        return False
    for claim in item.claims:
        # P569 - birth date
        if claim.property == "P569":
            too_young = context.data_time - timedelta(days=365 * 18)
            too_old = context.data_time - timedelta(days=365 * 110)
            date = claim.text(enricher)
            if date.text is None:
                continue
            if date.text > too_young.isoformat():
                # context.log.warning("Person is too young", qid=item.id, date=date.text)
                return False
            if date.text < too_old.isoformat():
                # context.log.warning("Person is too old", qid=item.id, date=date.text)
                return False
        # P570 - death date
        if claim.property == "P570":
            date = claim.text(enricher)
            # context.log.warning("Person is dead", qid=item.id, date=date)
            if date.text is not None:
                return False
        # print(item.id, claim.property, claim._value)
    return True


def apply_name(entity: Entity, item: Item) -> None:
    for lang in LANGUAGES:
        for label in item.labels:
            if label.lang == lang:
                entity.add("name", label.text, lang=lang)
                return


def crawl_category(
    context: Context, enricher: WikidataEnricher, category: Dict[str, Any]
) -> None:
    cache_days = int(category.pop("cache_days", 14))
    topics: List[str] = category.pop("topics", [])
    if "topic" in category:
        topics.append(category.pop("topic"))
    country: Optional[str] = category.pop("country", None)

    query = dict(QUERY)
    cat: str = category.pop("category", "")
    query["categories"] = cat.strip()
    query.update(category)

    position_data: Dict[str, Any] = category.pop("position", {})
    position: Optional[Entity] = None
    if "name" in position_data:
        position = h.make_position(context, **position_data, id_hash_prefix="wd-cat")

    query_string = urlencode(query)
    # print(query_string)
    url = f"{URL}?{query_string}"
    data = context.fetch_text(url, cache_days=cache_days)
    wrapper = StringIO(data)
    results = 0
    emitted = 0
    for row in csv.DictReader(wrapper):
        results += 1
        qid = row.pop("Wikidata")
        entity = context.make("Person")
        entity.id = qid
        entity.add("wikidataId", qid)
        item = enricher.fetch_item(qid)
        if not check_item_relevant(context, enricher, item):
            continue

        apply_name(entity, item)
        if not entity.has("name"):
            name = title_name(row.pop("title"))
            entity.add("name", name)
        entity.add("topics", topics)
        entity.add("country", country)
        context.emit(entity)
        if position is not None:
            occupancy = h.make_occupancy(context, entity, position)
            if occupancy is not None:
                context.emit(occupancy)

        emitted += 1

    if emitted > 0 and position is not None:
        context.emit(position)

    context.log.info(
        "PETScanning category: %s" % cat,
        topics=topics,
        results=results,
        emitted=emitted,
    )


def crawl(context: Context) -> None:
    enricher = WikidataEnricher(context.dataset, context.cache, context.dataset.config)
    categories: List[Dict[str, Any]] = context.dataset.config.get("categories", [])
    for category in categories:
        crawl_category(context, enricher, category)
