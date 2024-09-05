import json
import zipfile
from typing import Dict, Any
from followthemoney.types import registry
from zavod import Context
import re

from zavod.entity import Entity

IGNORE_SCHEMATA = ["Event", "Documentation", "Document"]
IGNORE_PROPS = ["proof"]

# Unified regex pattern to extract codes
REGEX_CODES = {
    "okpoCode": re.compile(
        r"All-Russian Classifier of Enterprises and Organizations \(OKPO\): (\d+)"
    ),
    "ogrnCode": re.compile(r"Primary State Registration Number \(OGRN\): (\d+)"),
    "innCode": re.compile(r"Taxpayer Identification Number \(INN\): (\d+)"),
    "kppCode": re.compile(r"Reason for Registration Code \(KPP\): (\d+)"),
}


def parse_identifiers(entity: Entity, properties: Dict[str, Any]) -> None:
    if "idNumber" in properties:
        id_numbers = properties.get("idNumber")
        new_id_numbers = []

        for value in id_numbers:
            for prop, regex in REGEX_CODES.items():
                match = regex.search(value)
                if match:
                    if prop not in entity.schema.properties:
                        continue
                    value = regex.sub("", value, 1)
                    if prop not in properties:
                        properties[prop] = []
                    properties[prop].append(match.group(1))
            if value:
                new_id_numbers.append(value)
        properties["idNumber"] = new_id_numbers


def parse_entity(context: Context, data: Dict[str, Any]) -> None:
    if data["schema"] in IGNORE_SCHEMATA:
        return

    entity = context.make(data["schema"])
    entity.id = context.make_slug(data["id"])

    properties = data.get("properties", {})
    parse_identifiers(entity, properties)

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
