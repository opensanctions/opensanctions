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

from osapi.settings import ES_INDEX, ES_URL, BASE_SCHEMA
from osapi.mapping import make_mapping, INDEX_SETTINGS, TEXT_TYPES
from osapi.data import get_scope, get_database

warnings.filterwarnings("ignore", category=ElasticsearchWarning)

log = logging.getLogger("osapi.index")
es = AsyncElasticsearch(hosts=[ES_URL])


async def generate_entities(index, loader):
    for idx, entity in enumerate(loader):
        if idx % 1000 == 0 and idx > 0:
            log.info("Index [%s]: %d entities...", index, idx)
        data = entity.to_dict()
        if entity.schema.is_a(BASE_SCHEMA):
            for _, adj in loader.get_adjacent(entity):
                for prop, value in adj.itervalues():
                    if prop.type in (registry.date, registry.entity):
                        continue
                    field = prop.type.group or "text"
                    if field not in data:
                        data[field] = []
                    data[field].append(value)

        entity_id = data.pop("id")
        yield {"_index": index, "_id": entity_id, "_source": data}


async def index():
    dataset = get_scope()

    latest = Statement.max_last_seen(dataset)
    ts = latest.strftime("%Y%m%d%H%M%S")
    prefix = f"{ES_INDEX}-"
    next_index = f"{prefix}{ts}"
    exists = await es.indices.exists(index=next_index)
    if exists:
        log.info("Index [%s] is up to date.", next_index)
        return
    schemata = Statement.all_schemata(dataset)
    mapping = make_mapping(schemata)
    log.info("Create index: %s", next_index)
    await es.indices.create(index=next_index, mappings=mapping, settings=INDEX_SETTINGS)
    db = get_database(cached=True)
    loader = db.view(dataset, assembler=export_assembler)
    await async_bulk(es, generate_entities(next_index, loader), stats_only=True)
    log.info("Indexing done, force merge")
    await es.indices.refresh(index=next_index)
    await es.indices.forcemerge(index=next_index)

    log.info("Index [%s] is now aliased to: %s", next_index, ES_INDEX)
    await es.indices.put_alias(index=next_index, name=ES_INDEX)

    indices = await es.cat.indices(format="json")
    for index in indices:
        name = index.get("index")
        if name.startswith(prefix) and name != next_index:
            log.info("Delete existing index: %s", name)
            await es.indices.delete(index=name)


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


def entity_query(dataset: Dataset, entity: Entity, fuzzy: bool = False):
    terms: Dict[str, List[str]] = {}
    texts: List[str] = []
    for prop, value in entity.itervalues():
        if prop.type.group is not None:
            if prop.type not in TEXT_TYPES:
                field = prop.type.group
                if field not in terms:
                    terms[field] = []
                terms[field].append(value)
        texts.append(value)

    shoulds: List[Dict[str, Any]] = []
    for field, texts in terms.items():
        shoulds.append({"terms": {field: texts}})
    for text in texts:
        shoulds.append({"match_phrase": {"text": text}})
    return filter_query(shoulds, dataset, entity.schema)


def text_query(dataset: Dataset, schema: Schema, query: str, fuzzy: bool = False):
    should = {
        "query_string": {
            "query": query,
            "default_field": "text",
            "default_operator": "and",
            "fuzziness": 1,
            "lenient": fuzzy,
        }
    }
    return filter_query([should], dataset, schema)


def result_entity(data) -> Tuple[Entity, float]:
    source = data.get("_source")
    source["id"] = data.get("_id")
    return Entity.from_dict(model, source), data.get("_score")


def result_entities(result) -> Generator[Tuple[Entity, float], None, None]:
    hits = result.get("hits", {})
    for hit in hits.get("hits", []):
        yield result_entity(hit)


async def query_entities(query: Dict[Any, Any], limit: int = 5):
    # pprint(query)
    resp = await es.search(index=ES_INDEX, query=query, size=limit)
    for entity, score in result_entities(resp):
        yield entity, score


async def get_entity(entity_id: str) -> Optional[Entity]:
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
    dataset: Dataset, query: Dict[Any, Any], limit: int, nested: bool
):
    results = []
    async for result, score in query_entities(query, limit=limit):
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
