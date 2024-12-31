import json
import zipfile
from typing import Dict, Any, List
from followthemoney.types import registry
from zavod import Context

IGNORE_SCHEMATA = ["Event", "Documentation", "Document"]
IGNORE_PROPS = ["proof"]


def parse_entity(context: Context, data: Dict[str, Any]) -> None:
    if data["schema"] in IGNORE_SCHEMATA:
        return

    entity = context.make(data["schema"])
    entity.id = context.make_slug(data["id"])

    properties: Dict[str, List[str]] = data.pop("properties", {})
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

        for value in values:
            prop_ = prop
            original_value = value
            if prop.type == registry.entity:
                value = context.make_slug(value)
            if prop.name == "idNumber":
                if ":" not in value:
                    context.log.warn(
                        "Invalid idNumber value",
                        entity=entity,
                        value=value,
                    )
                else:
                    scheme, value = value.split(":", 1)
                    res = context.lookup_value("id_scheme", scheme)
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

            entity.add(prop_, value, original_value=original_value)

    context.emit(entity)


def crawl(context: Context) -> None:
    path = context.fetch_resource("data.zip", context.data_url)
    context.export_resource(path, "application/zip", title=context.SOURCE_TITLE)

    with zipfile.ZipFile(path, "r") as zipfh:
        for name in zipfh.namelist():
            if not name.endswith(".json"):
                continue
            with zipfh.open(name, "r") as fh:
                while line := fh.readline():
                    data = json.loads(line)
                    parse_entity(context, data)
