from followthemoney import model
from followthemoney.types import registry

DATE_FORMAT = "yyyy-MM-dd'T'HH||yyyy-MM-dd'T'HH:mm||yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd||yyyy-MM||yyyy"
TEXT_TYPES = (registry.name, registry.address, registry.url)
INDEX_SETTINGS = {
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
    },
    "index": {"refresh_interval": "-1"},
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
    if type_ in TEXT_TYPES:
        field_type = "text"
    if index is None:
        index = type_.matchable
    return make_field(field_type, copy_to=copy_to, index=index)


def make_mapping(schemata):
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
        "text": make_field("text", index=True),
        "last_seen": make_field("date", format=DATE_FORMAT),
        "first_seen": make_field("date", format=DATE_FORMAT),
        "properties": {"dynamic": "strict", "properties": prop_mapping},
    }
    for t in registry.groups.values():
        mapping[t.group] = make_type_field(t, index=True, copy_to="text")

    drop_fields = [t.group for t in registry.groups.values()]
    drop_fields.append("text")
    return {
        "dynamic": "strict",
        "properties": mapping,
        "_source": {"excludes": drop_fields},
    }
