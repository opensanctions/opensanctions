import re
import json
from typing import Any, Dict
from rigour.mime.types import JSON

from zavod import Context
from zavod import helpers as h

NO_YEAR = re.compile(r"^\d{1,2}\.\d{1,2}\.?$")
IGNORE_COLUMNS = [
    "canada",
    "us",
    "uk",
    "eu",
    "monaco",
    "switzerland",
    "australia",
    "japan",
    "korea",
]


def parse_result(context: Context, row: Dict[str, Any]):
    name_en = row.pop("name_en")
    result = context.lookup("censored", name_en)
    if result is not None:
        return
    name_ru = row.pop("name_ru")
    dob = row.pop("birthdate")
    published_at = row.pop("published_at")
    entity = context.make("Person")
    entity.id = context.make_id(name_en, name_ru, published_at, dob)
    entity.add("name", name_en, lang="eng")
    entity.add("name", h.remove_bracketed(name_en), lang="eng")
    entity.add("topics", "poi")
    entity.add("alias", name_ru, lang="rus")
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

    transliterations = row.pop("transliterations")
    for tl in transliterations.split("\n"):
        tl = h.remove_bracketed(tl)
        entity.add("alias", tl)
    if dob is not None and not NO_YEAR.match(dob):
        h.apply_date(entity, "birthDate", dob)
    entity.add("gender", row.pop("gender"))
    entity.add("createdAt", published_at)

    context.emit(entity)
    context.audit_data(row, ignore=IGNORE_COLUMNS)


def crawl(context: Context):
    path = context.fetch_resource("source.json", context.data_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as file:
        data = json.load(file)
        for result in data:
            parse_result(context, result)
