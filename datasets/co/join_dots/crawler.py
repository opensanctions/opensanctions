from normality import collapse_spaces, slugify
from pantomime.types import CSV
from typing import Dict
import csv
from lxml import html

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
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
    entity.id = context.make_slug("person", id_number)
    h.apply_name(
        entity,
        first_name=row.pop("Primer Nombre"),
        second_name=row.pop("Segundo Nombre"),
        patronymic=row.pop("Primer Apellido"),
        matronymic=row.pop("Segundo Apellido"),
    )
    entity.add("idNumber", id_number)
    return slugify(entity.get("name")), entity


def parse_node(context: Context, node: str) -> Dict[str, str]:
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

    links = doc.findall(".//a")
    if len(links) == 1:
        data["link"] = links[0].get("href")
    else:
        context.log.warning("Expected exactly one link for node", id=node["ID"], count=len(links))
    return data


def crawl_node(context: Context, peps: Dict[str, Entity], node: Dict[str, str]):
    data = parse_node(context, node)
    if data:
        name = data["politically_exposed_person"]
        name_slug = slugify(name)
        entity = peps.get(name_slug, None)
        if entity:
            position_slug = slugify(data.pop("position"))
            res = context.lookup("position", position_slug)
            if res:
                notes = ""
                red_flags = data.pop("red_flags_found", None)
                if red_flags is not None:
                    notes += f"Potential red flags: {red_flags}."
                ownership = data.get("corporation_shares", None)
                if ownership and ownership != "null":
                    notes += f" Corporation shares: {ownership}"
                if notes:
                    entity.add("notes", notes)
                entity.add("sourceUrl", data.pop("link"))

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
            context.log.info("PEP not found", name=name)    


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    peps = {}
    dupes = set()

    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            name_slug, entity = crawl_row(context, row)
            if name_slug in peps:
                context.log.info("Dropping name with duplicate entry", name=name_slug)
                dupes.add(name_slug)
                peps.pop((name_slug))
            else:
                if name_slug not in dupes:
                    peps[name_slug] = entity

    # Only try and import the PEPs for whom we have position information
    graph = context.fetch_json(GRAPH_URL, cache_days=1)
    for node in graph["nodes"]:
        if node["Type"] in ["PEP", "FLAG"]:
            crawl_node(context, peps, node)
