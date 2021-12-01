import asyncio
import logging
import warnings
from pprint import pprint
from typing import Any, AsyncGenerator, Dict, Generator, List, Optional, Tuple
from elasticsearch import AsyncElasticsearch, TransportError
from elasticsearch.exceptions import ElasticsearchWarning
from elasticsearch.helpers import async_bulk
from followthemoney import model
from followthemoney.schema import Schema
from followthemoney.property import Property
from followthemoney.types import registry

from opensanctions.core import configure_logging, Dataset
from opensanctions.core.entity import Entity
from opensanctions.exporters import export_assembler
from opensanctions.model import Statement

from osapi.settings import ES_INDEX, ES_URL
from osapi.mapping import make_mapping, INDEX_SETTINGS, TEXT_TYPES
from osapi.data import get_scope, get_database

warnings.filterwarnings("ignore", category=ElasticsearchWarning)

log = logging.getLogger("osapi.index")
es = AsyncElasticsearch(hosts=[ES_URL])


async def generate_entities(loader):
    for idx, entity in enumerate(loader):
        if idx % 1000 == 0 and idx > 0:
            log.info("Index [%s]: %d entities...", ES_INDEX, idx)
        data = entity.to_dict()
        for _, adj in loader.get_adjacent(entity):
            for prop, value in adj.itervalues():
                if prop.type in (registry.date, registry.entity):
                    continue
                field = prop.type.group or "text"
                if field not in data:
                    data[field] = []
                data[field].append(value)

        entity_id = data.pop("id")
        yield {"_index": ES_INDEX, "_id": entity_id, "_source": data}


async def index():
    exists = await es.indices.exists(index=ES_INDEX)
    if exists:
        log.info("Delete existing index: %s", ES_INDEX)
        await es.indices.delete(index=ES_INDEX)

    dataset = get_scope()
    schemata = Statement.all_schemata(dataset)
    mapping = make_mapping(schemata)
    log.info("Create index: %s", ES_INDEX)
    await es.indices.create(index=ES_INDEX, mappings=mapping, settings=INDEX_SETTINGS)
    db = get_database(cached=True)
    loader = db.view(dataset, assembler=export_assembler)
    await async_bulk(es, generate_entities(loader), stats_only=True)
    log.info("Indexing done, force merge")
    await es.indices.refresh(index=ES_INDEX)
    await es.indices.forcemerge(index=ES_INDEX)


def filter_query(shoulds, dataset: Dataset, schema: Optional[Schema] = None):
    filters = [{"terms": {"datasets": dataset.source_names}}]
    if schema is not None:
        schemata = schema.matchable_schemata
        schemata.add(schema)
        if not schema.matchable:
            schemata.update(schema.descendants)
        names = [s.name for s in schemata]
        filters.append({"terms": {"schema": names}})
    return {"bool": {"filter": filters, "should": shoulds, "minimum_should_match": 1}}


def result_entity(data) -> Tuple[Entity, float]:
    source = data.get("_source")
    source["id"] = data.get("_id")
    return Entity.from_dict(model, source), data.get("_score")


def result_entities(result) -> Generator[Tuple[Entity, float], None, None]:
    hits = result.get("hits", {})
    for hit in hits.get("hits", []):
        yield result_entity(hit)


async def match_entities(
    dataset: Dataset, query: Entity, limit: int = 5, fuzzy: bool = False
) -> AsyncGenerator[Tuple[Entity, float], None]:
    terms = {}
    texts = []
    for prop, value in query.itervalues():
        if prop.type.group is not None:
            if prop.type not in TEXT_TYPES:
                field = prop.type.group
                if field not in terms:
                    terms[field] = []
                terms[field].append(value)
        texts.append(value)

    shoulds = []
    for field, texts in terms.items():
        shoulds.append({"terms": {field: texts}})
    for text in texts:
        shoulds.append({"match_phrase": {"text": text}})
    filtered = filter_query(shoulds, dataset, query.schema)
    resp = await es.search(index=ES_INDEX, query=filtered, size=limit)
    for entity, score in result_entities(resp):
        yield entity, score


async def get_entity(dataset: Dataset, entity_id: str) -> Optional[Entity]:
    data = await es.get(index=ES_INDEX, id=entity_id)
    entity, _ = result_entity(data)
    return entity


async def get_adjacent(
    dataset: Dataset, entity: Entity
) -> AsyncGenerator[Tuple[Property, Entity], None]:
    entities = entity.get_type_values(registry.entity)
    if len(entities):
        resp = await es.mget(index=ES_INDEX, body={"ids": entities})
        for raw in resp.get("docs", []):
            adj, _ = result_entity(raw)
            for prop, value in entity.itervalues():
                if prop.type == registry.entity and value == adj.id:
                    yield prop, adj

    query = {"term": {"entities": entity.id}}
    filtered = filter_query([query], dataset)
    resp = await es.search(index=ES_INDEX, query=filtered, size=9999)
    for adj, _ in result_entities(resp):
        for prop, value in adj.itervalues():
            if prop.type == registry.entity and value == entity.id:
                if prop.reverse is not None:
                    yield prop.reverse, adj


async def _to_nested_dict(
    dataset: Dataset, entity: Entity, depth: int, path: List[str]
) -> Dict[str, Any]:
    next_depth = depth if entity.schema.edge else depth - 1
    next_path = path + [entity.id]
    data = entity.to_dict()
    if next_depth < 0:
        return data
    nested: Dict[str, Any] = {}
    async for prop, adjacent in get_adjacent(dataset, entity):
        if adjacent.id in next_path:
            continue
        value = await _to_nested_dict(dataset, adjacent, next_depth, next_path)
        if prop.name not in nested:
            nested[prop.name] = []
        nested[prop.name].append(value)
    data["properties"].update(nested)
    return data


async def serialize_entity(
    dataset: Dataset, entity: Entity, nested: bool = False
) -> Dict[str, Any]:
    depth = 1 if nested else -1
    return await _to_nested_dict(dataset, entity, depth=depth, path=[])


async def query_results(
    dataset: Dataset, query: Entity, limit: int, fuzzy: bool, nested: bool
):
    results = []
    async for result, score in match_entities(dataset, query, limit=limit, fuzzy=fuzzy):
        data = await serialize_entity(dataset, result, nested=nested)
        data["score"] = score
        results.append(data)
    return results


async def get_index_stats() -> Dict[str, Any]:
    stats = await es.indices.stats(index=ES_INDEX)
    return stats.get("indices", {}).get(ES_INDEX)


async def get_index_status() -> bool:
    try:
        health = await es.cluster.health(index=ES_INDEX)
        return health.get("status") in ("yellow", "green")
    except TransportError:
        return False


if __name__ == "__main__":
    configure_logging(level=logging.INFO)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(index())
