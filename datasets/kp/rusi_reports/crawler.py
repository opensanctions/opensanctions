import json
import zipfile
from typing import Dict, Any
from followthemoney.types import registry
from zavod import Context

IGNORE_SCHEMATA = ["Event", "Documentation", "Document"]
IGNORE_PROPS = ["proof"]


def parse_entity(context: Context, data: Dict[str, Any]) -> None:
    if data["schema"] in IGNORE_SCHEMATA:
        return
    entity = context.make(data["schema"])
    entity.id = context.make_slug(data["id"])
    for prop_name, values in data["properties"].items():
        prop = entity.schema.get(prop_name)
        if prop_name in IGNORE_PROPS:
            continue
        if prop is None:
            alias_prop = context.lookup_value("props", prop_name)
            if alias_prop is None:
                context.log.warn("Unknown property: %s" % prop_name, entity=entity)
                continue
            prop = entity.schema.get(alias_prop)
            assert prop is not None
        for value in values:
            if prop.type == registry.entity:
                value = context.make_slug(value)
            entity.unsafe_add(prop, value, cleaned=True)
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
