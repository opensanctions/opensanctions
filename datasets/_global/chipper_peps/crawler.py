from normality import collapse_spaces
from pantomime.types import CSV
from typing import Dict
import csv

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.logic.pep import OccupancyStatus

FORMATS = ["%m/%d/%Y"]


def emit_position(context: Context, entity: Entity, name: str):
    position = h.make_position(context, name, country="ng")
    occupancy = h.make_occupancy(
        context, entity, position, False, status=OccupancyStatus.UNKNOWN
    )
    context.emit(position)
    context.emit(occupancy)


def crawl_row(context: Context, row: Dict[str, str]):
    entity = context.make("Person")
    identifier = row.pop("Unique Identifier")
    if not identifier:
        return
    entity.id = context.make_slug(identifier)
    h.apply_name(
        entity,
        first_name=row.pop("First Name"),
        middle_name=row.pop("Middle Name"),
        last_name=row.pop("Last Name"),
    )
    entity.add("title", row.pop("Title"))
    entity.add("gender", row.pop("Gender"))
    entity.add("birthDate", h.parse_date(row.pop("Date of Birth"), FORMATS))
    entity.add("address", row.pop("Official Address"))
    entity.add("birthPlace", row.pop("State Of Origin"))

    previous_pos = row.pop("Previous Position")
    if previous_pos:
        emit_position(context, entity, previous_pos)
    present_pos = row.pop("Present Position")
    if present_pos:
        emit_position(context, entity, present_pos)

    context.emit(entity)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
