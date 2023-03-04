import json
from typing import Any, Dict
from pantomime.types import JSON

from opensanctions.core import Context
from opensanctions import helpers as h

DATE_FORMATS = ["%d.%m.%Y", "%d-%m-%Y"]


def parse_result(context: Context, row: Dict[str, Any]):
    entity = context.make("Person")
    # context.inspect(row)
    for tag in row.pop("tags"):
        result = context.lookup("tags", tag["slug"])
        if result is not None:
            entity.add("position", result.value)
        else:
            entity.add("position", tag["name_en"], lang="eng")
            entity.add("position", tag["name_ru"], lang="rus")

        for leaf in tag["leaf_nodes"]:
            if leaf["slug"] == "oligarchs":
                entity.add("topics", "role.oligarch")
            description = leaf["description"]
            result = context.lookup("descriptions", description)
            if result is not None:
                description = result.values
            entity.add("notes", description)

    name_en = row.pop("name_en")
    name_ru = row.pop("name_ru")
    dob = row.pop("birthdate")
    published_at = row.pop("published_at")
    entity.id = context.make_id(name_en, name_ru, published_at, dob)
    entity.add("name", name_en, lang="eng")
    entity.add("name", h.remove_bracketed(name_en), lang="eng")
    entity.add("alias", name_ru, lang="rus")
    transliterations = row.pop("transliterations")
    for tl in transliterations.split("\n"):
        tl = h.remove_bracketed(tl).strip()
        entity.add("alias", tl)
    entity.add("birthDate", h.parse_date(dob, DATE_FORMATS), original_value=dob)
    entity.add("gender", row.pop("gender"))
    entity.add("createdAt", published_at)

    context.emit(entity, target=True)
    # context.inspect(row)


def crawl(context: Context):
    path = context.fetch_resource("source.json", context.source.data.url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as file:
        data = json.load(file)
        for result in data:
            parse_result(context, result)
