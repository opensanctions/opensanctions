import asyncio
from pprint import pprint
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
from followthemoney import model
from followthemoney.types import registry

from opensanctions.core import Dataset
from opensanctions.core.loader import Database
from opensanctions.core.resolver import get_resolver
from opensanctions.model import Statement

INDEX = "opensanctions-test-1"
es = AsyncElasticsearch()
dataset = Dataset.require("default")
resolver = get_resolver()
db = Database(dataset, resolver, cached=True)

DATE_FORMAT = "yyyy-MM-dd'T'HH||yyyy-MM-dd'T'HH:mm||yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd||yyyy-MM||yyyy"
SETTINGS = {
    "analysis": {
        "normalizer": {
            "osa-normalizer": {
                "type": "custom",
                "filter": ["lowercase", "asciifolding"],
            }
        },
        "analyzer": {
            "osa-analyzer": {
                "tokenizer": "standard",
                "filter": ["lowercase", "asciifolding"],
            }
        },
    }
}


def make_field(type_, copy_to=None, index=None, format=None):
    spec = {"type": type_}
    if type_ == "keyword":
        spec["normalizer"] = "osa-normalizer"
    if type_ == "text":
        spec["analyzer"] = "osa-analyzer"
    if copy_to is not None and copy_to is not False:
        spec["copy_to"] = copy_to
    if index is not None:
        spec["index"] = index
    if format is not None:
        spec["format"] = format
    return spec


def make_type_field(type_, copy_to=True, index=None):
    if type_ == registry.date:
        return make_field("date", copy_to=copy_to, format=DATE_FORMAT)
    strong = type_.group is not None
    field_type = "keyword" if strong else "text"
    if type_ in (registry.name, registry.address, registry.url):
        field_type = "text"
    if index is None:
        index = type_.matchable
    return make_field(field_type, copy_to=copy_to, index=index)


def make_mapping(dataset):
    schemata = Statement.all_schemata(dataset)

    prop_mapping = {}
    for schema_name in schemata:
        schema = model.get(schema_name)
        for name, prop in schema.properties.items():
            if prop.stub:
                continue
            copy_to = ["text"]
            if prop.type.group is not None:
                copy_to.append(prop.type.group)
            prop_mapping[name] = make_type_field(prop.type, copy_to=copy_to)

    mapping = {
        "schema": make_field("keyword"),
        "caption": make_field("keyword", copy_to=["names", "text"]),
        "datasets": make_field("keyword"),
        "referents": make_field("keyword"),
        "target": make_field("boolean"),
        "text": make_field("text"),
        "last_seen": make_field("date", format=DATE_FORMAT),
        "first_seen": make_field("date", format=DATE_FORMAT),
        "properties": {"dynamic": "strict", "properties": prop_mapping},
    }
    for type_ in registry.groups.values():
        mapping[type_.group] = make_type_field(type_, index=True, copy_to="text")

    drop_fields = [t.group for t in registry.groups.values()]
    drop_fields.append("text")
    return {
        "dynamic": "strict",
        "properties": mapping,
        "_source": {"excludes": drop_fields},
    }


async def generate_entities():
    loader = db.view(dataset)
    for idx, entity in enumerate(loader):
        if idx % 1000 == 0 and idx > 0:
            print("INDEXED", idx)
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
        yield {"_index": INDEX, "_id": entity_id, "_source": data}


async def index():
    print("INDEX CREATE")
    exists = await es.indices.exists(index=INDEX)
    if exists:
        await es.indices.delete(index=INDEX)

    mapping = make_mapping(dataset)
    await es.indices.create(index=INDEX, mappings=mapping, settings=SETTINGS)
    # await es.create(index=INDEX)
    print("INDEX GENERATE")
    await async_bulk(es, generate_entities(), stats_only=True)
    await es.indices.forcemerge(index=INDEX)


async def main():
    # await index()
    resp = await es.search(
        index=INDEX,
        query={"match_all": {}},
        size=20,
    )
    pprint(resp)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
