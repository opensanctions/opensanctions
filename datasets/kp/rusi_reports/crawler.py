import json
import zipfile
from typing import Dict, Any
from followthemoney.types import registry
from zavod import Context
import re

IGNORE_SCHEMATA = ["Event", "Documentation", "Document"]
IGNORE_PROPS = ["proof"]

# Unified regex pattern to extract codes
REGEX_CODES = re.compile(
    r"All-Russian Classifier of Enterprises and Organizations \(OKPO\): (\d+)|"
    r"Primary State Registration Number \(OGRN\): (\d+)|"
    r"Taxpayer Identification Number \(INN\): (\d+)|"
    r"Reason for Registration Code \(KPP\): (\d+)"
)


def extract_codes(text: str) -> Dict[str, str]:
    matches = REGEX_CODES.findall(text)
    # Ensure matches are not empty and construct the dictionary safely
    codes = {}
    for match in matches:
        if match[0]:  # OKPO match
            codes["okpoCode"] = match[0]
        if match[1]:  # OGRN match
            codes["ogrnCode"] = match[1]
        if match[2]:  # INN match
            codes["innCode"] = match[2]
        if match[3]:  # KPP match
            codes["kppCode"] = match[3]

    return codes


def parse_entity(context: Context, data: Dict[str, Any]) -> None:
    if data["schema"] in IGNORE_SCHEMATA:
        return
    entity = context.make(data["schema"])
    entity.id = context.make_slug(data["id"])

    idNumberCodes = {}
    processed_id_numbers = set()

    for prop_name, values in data["properties"].items():
        prop = entity.schema.get(prop_name)
        if prop_name in IGNORE_PROPS:
            continue

        if prop_name == "idNumber":
            for value in values:
                codes = extract_codes(value)
                for code_type, code_value in codes.items():
                    if code_type == "kppCode" and entity.schema.name != "Company":
                        continue  # Skip adding KPP code if the entity is not a Company
                    idNumberCodes[code_type] = code_value
                    entity.add(code_type, code_value)
                    # Filter out processed idNumbers
            values = [v for v in values if v not in processed_id_numbers]

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
