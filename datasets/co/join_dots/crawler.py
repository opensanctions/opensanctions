from normality import collapse_spaces, slugify
from pantomime.types import CSV
from typing import Dict
import csv
from lxml import html

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import OccupancyStatus

# ID / CC
# Primer Nombre
# Segundo Nombre
# Primer Apellido
# Segundo Apellido
# ID / Entidad
# Entidad
# Departamento
# Municipio


# First name
# Second name
# Surname
# Second surname
# Identity
# Entity
# Department
# Municipality

GRAPH_URL = "https://peps.directoriolegislativo.org/json/graph.json"


def crawl_row(context: Context, row: Dict[str, str]):
    entity = context.make("Person")
    id_number = row.pop("ID / CC")
    entity.id = context.make_slug(id_number)
    h.apply_name(
        entity,
        first_name=row.pop("Primer Nombre"),
        second_name=row.pop("Segundo Nombre"),
        patronymic=row.pop("Primer Apellido"),
        matronymic=row.pop("Segundo Apellido"),
    )
    entity.add("idNumber", id_number)
    return slugify(entity.get("name")), entity


def parse_node(node: str) -> Dict[str, str]:
    description = node["Description"]
    if description.strip() == "":
        return None
    doc = html.fromstring(description)
    text: str = doc.text_content()
    data = {}
    for row in text.split("\n"):
        if row:
            key, value = row.split(":", 1)
            data[slugify(key, "_")] = collapse_spaces(value)
    return data


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    pep = {}
    dupes = set()

    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            name_slug, entity = crawl_row(context, row)
            if name_slug in pep:
                context.log.info("Dropping name with duplicate entry", name=name_slug)
                dupes.add(name_slug)
                pep.pop((name_slug))
            else:
                if name_slug not in dupes:
                    pep[name_slug] = entity

    graph = context.fetch_json(GRAPH_URL, cache_days=1)
    for node in graph["nodes"]:
        if node["Type"] in ["PEP", "FLAG"]:
            data = parse_node(node)
            if data:
                # print(data)
                name = data["politically_exposed_person"]
                name_slug = slugify(name)
                print(name_slug)
                entity = pep.get(name_slug, None)
                if entity:
                    position_slug = slugify(data.get("position"))
                    res = context.lookup("position", position_slug)
                    if res:
                        print("######### emitting", name)
                        print(slugify(data.get("position")))
                        print("red flags:", data.get("red_flags_found"))
                        print("ownerships:", data.get("corporation_shares"))
                        print()
                        position = h.make_position(
                            context, res.name, country="co", topics=res.topics
                        )
                        occupancy = h.make_occupancy(
                            context, entity, position, status=OccupancyStatus.UNKNOWN
                        )
                        context.emit(occupancy)
                        context.emit(position)
                        context.emit(entity, target=True)
                    else:
                        print(position_slug)
                        print()
                else:
                    context.log.info("PEP not found", name=name)
