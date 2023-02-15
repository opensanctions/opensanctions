import json
from typing import Any, Dict
from pantomime.types import JSON

from opensanctions.core import Context


def parse_result(context: Context, row: Dict[str, Any]):
    entity = context.make("Person")
    tags = row.pop("tags")
    concattags = []
    for tag in tags:
        concattags.append(tag["name_en"])
        if tag["slug"] != "individuals-involved-in-corruption":
            entity.add("position", tag["name_en"])
        for leaf in tag["leaf_nodes"]:
            if leaf["slug"] == "oligarchs":
            entity.add("topics", "role.oligarch")
            description = leaf["description"]
            result = context.lookup("descriptions", description)
            if result is not None:
                description = result.values
            entity.add("notes", description)

    name_en = row.pop("name_en")
    dob = row.pop("birthdate")
    entity.id = context.make_id(name_en, "\n".join(concattags), dob)
    entity.add("name", name_en)
    entity.add("alias", row.pop("name_ru"))
    transliterations = row.pop("transliterations")
    for tl in transliterations.split("\n"):
        tl = tl.strip()
        if tl:
            entity.add("alias", tl)
    entity.add("birthDate", dob)
    entity.add("gender", row.pop("Gender"))

    context.emit(entity, target=True)
    # context.inspect(row)


def crawl(context: Context):
    path = context.fetch_resource("source.json", context.source.data.url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as file:
        data = json.load(file)
        for result in data:
            parse_result(context, result)
