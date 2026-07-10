import json
from typing import Any
from followthemoney.property import Property
from followthemoney.types import registry
from rigour.mime.types import JSON

from zavod import Context

# Schemata we don't ingest. Documentation/Document/Event/Source are the source's
# provenance graph (which reports mention an entity); we keep only the entity
# network. Sanction designations are intentionally excluded from this dataset.
IGNORE_SCHEMATA = ["Event", "Documentation", "Document", "Source", "Sanction"]
IGNORE_PROPS = ["proof"]

# Relationship edges that only exist in the source's `edges` array (they have no
# node record of their own), mapped to the FTM edge's (source, target) properties.
EDGE_SCHEMATA: dict[str, tuple[str, str]] = {
    "Employment": ("employee", "employer"),
    "Membership": ("member", "organization"),
    "Associate": ("person", "associate"),
    "UnknownLink": ("subject", "object"),
}


def parse_entity(context: Context, data: dict[str, Any]) -> None:
    if data["schema"] in IGNORE_SCHEMATA:
        return

    entity = context.make(data["schema"])
    entity.id = context.make_slug(data["id"])

    properties: dict[str, list[str]] = data.pop("properties", {})
    for prop_name, values in properties.items():
        if prop_name in IGNORE_PROPS:
            continue
        prop = entity.schema.get(prop_name)
        if prop is None:
            alias_prop = context.lookup_value("props", prop_name)
            if alias_prop is None:
                context.log.warn(f"Unknown property: {prop_name}", entity=entity)
                continue
            prop = entity.schema.get(alias_prop)
            assert prop is not None, f"No valid FtM property found for {prop_name!r}"

        for value in values:
            prop_: Property | None = prop
            original_value = value
            if prop.type == registry.entity:
                slug = context.make_slug(value)
                assert slug is not None
                value = slug
            if prop.name == "idNumber":
                if ":" not in value:
                    context.log.warn(
                        "Invalid idNumber value",
                        entity=entity,
                        value=value,
                    )
                else:
                    scheme, value = value.split(":", 1)
                    res = context.lookup_value("id_scheme", scheme.strip())
                    if res is None:
                        context.log.warn(
                            "Unknown id scheme",
                            entity=entity,
                            scheme=scheme,
                            value=value,
                        )
                    else:
                        if res == "kppCode":
                            entity.add_schema("Company")
                        prop_ = entity.schema.get(res)

            assert prop_ is not None
            entity.add(prop_, value, original_value=original_value)

    context.emit(entity)


def parse_edge(context: Context, data: dict[str, Any]) -> None:
    schema = data.pop("schema")
    if schema not in EDGE_SCHEMATA:
        return
    source_prop, target_prop = EDGE_SCHEMATA[schema]
    source = data.pop("source")
    target = data.pop("target")
    role = data.pop("role").strip()
    relationship = data.pop("relationship").strip()

    entity = context.make(schema)
    entity.id = context.make_id(schema, source, target, role, relationship)
    entity.add(source_prop, context.make_slug(source))
    entity.add(target_prop, context.make_slug(target))
    if role:
        entity.add("role", role)
    if relationship:
        entity.add("relationship", relationship)
    context.audit_data(data)
    context.emit(entity)


def crawl(context: Context) -> None:
    path = context.fetch_resource("data.json", context.data_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)

    with open(path, "r") as fh:
        data = json.load(fh)

    for item in data["entities"]:
        parse_entity(context, item)
    for edge in data["edges"]:
        parse_edge(context, edge)
