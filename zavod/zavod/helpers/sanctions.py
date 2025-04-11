from typing import Optional

from zavod.context import Context
from zavod.entity import Entity
from zavod import helpers as h
from zavod import settings
from zavod.stateful import programs

ALWAYS_FORMATS = ["%Y-%m-%d", "%Y-%m", "%Y"]


def lookup_sanction_program_key(
    context: Context, source_key: Optional[str]
) -> Optional[str]:
    """Lookup the sanction program key based on the source key."""
    program_key = context.lookup_value("sanction.program", source_key)
    if program_key is None:
        context.log.warn(f"Program key {program_key!r} not found.")
    return program_key


def make_sanction(
    context: Context,
    entity: Entity,
    key: Optional[str] = None,
    program_name: Optional[str] = None,
    program_key: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Entity:
    """Create and return a sanctions object derived from the dataset metadata.

    The country, authority, sourceUrl, and subject entity properties
    are automatically set.

    Args:
        context: The runner context with dataset metadata.
        entity: The entity to which the sanctions object will be linked.
        key: An optional key to be included in the ID of the sanction.
        program_name: An optional program name.
        program_key: An optional OpenSanction program key.
        start_date: An optional start date for the sanction.
        end_date: An optional end date for the sanction.

    Returns:
        A new entity of type Sanction.
    """
    assert entity.schema.is_a("Thing"), entity.schema
    assert entity.id is not None, entity.id
    dataset = context.dataset
    assert dataset.publisher is not None
    sanction = context.make("Sanction")
    sanction.id = context.make_id("Sanction", entity.id, key)
    sanction.add("entity", entity)
    if dataset.publisher.country != "zz":
        sanction.add("country", dataset.publisher.country)
    sanction.add("authority", dataset.publisher.name)
    sanction.add("sourceUrl", dataset.url)

    if program_name is not None:
        sanction.set("program", program_name)

    if program_key is not None:
        program = programs.get_program_by_key(context, program_key)
        if program is not None:
            sanction.add("programId", program_key)
            if not program_name:
                sanction.set("program", program.title)
            if program.url:
                sanction.add("programUrl", program.url)
            else:
                sanction.add(
                    "programUrl",
                    f"https://www.opensanctions.org/programs/{program_key}",
                )
        else:
            context.log.warn(f"Program key {program_key!r} not found.")
    if start_date is not None:
        h.apply_date(sanction, "startDate", start_date)
    if end_date is not None:
        h.apply_date(sanction, "endDate", end_date)
        iso_end_date = max(sanction.get("endDate"))
        is_active = iso_end_date >= settings.RUN_TIME_ISO
        sanction.add("status", "active" if is_active else "inactive")

    return sanction


def is_active(sanction: Entity) -> bool:
    """Check if a sanction is currently active.

    A sanction is active if the current time is between its earliest start date and latest end date.

    Args:
        sanction: The sanction entity to check.
    """
    iso_start_date = min(sanction.get("startDate"), default=None)
    iso_end_date = max(sanction.get("endDate"), default=None)
    is_active = (
        iso_start_date is None or iso_start_date <= settings.RUN_TIME_ISO
    ) and (iso_end_date is None or iso_end_date >= settings.RUN_TIME_ISO)
    return is_active
