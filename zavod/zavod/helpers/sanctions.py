from rigour.dates import ended_before, starts_after

from zavod import helpers as h
from zavod import settings
from zavod.constants import ORIGIN_METADATA
from zavod.context import Context
from zavod.entity import Entity
from zavod.stateful import programs

ALWAYS_FORMATS = ["%Y-%m-%d", "%Y-%m", "%Y"]


def lookup_sanction_program_key(context: Context, source_key: str | None) -> str | None:
    """Lookup the sanction program key based on the source key."""
    res = context.lookup("sanction.program", source_key)
    if res is None:
        context.log.warn(f"Program key for source key {source_key!r} not found.")
        return None
    return res.value


def make_sanction(
    context: Context,
    entity: Entity,
    key: str | None = None,
    program_name: str | None = None,
    source_program_key: str | None = None,
    program_key: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> Entity:
    """Create and return a sanctions object derived from the dataset metadata.

    The country, authority, sourceUrl, and subject entity properties
    are automatically set.

    If an ``end_date`` is given, a ``status`` of "active" or "inactive" is
    derived using the same semantics as `is_active`. Note that the status is
    only computed at construction time: dates applied to the sanction
    afterwards (e.g. via `h.apply_date`) do not update it.

    Args:
        context: The runner context with dataset metadata.
        entity: The entity to which the sanctions object will be linked.
        key: An optional key to be included in the ID of the sanction.
        program_name: An optional program name.
        program_key: An optional OpenSanction program key.
        source_program_key: Program key at the source, will be set as the original value for programId.
        start_date: An optional start date for the sanction.
        end_date: An optional end date for the sanction.

    Returns:
        A new entity of type Sanction.
    """
    assert entity.schema.is_a("Thing"), entity.schema
    assert entity.id is not None, entity.id
    dataset = context.dataset.model
    assert dataset.publisher is not None
    sanction = context.make("Sanction")
    sanction.id = context.make_id("Sanction", entity.id, key)
    sanction.add("entity", entity)
    if dataset.publisher.country != "zz":
        sanction.add("country", dataset.publisher.country, origin=ORIGIN_METADATA)
    sanction.add("authority", dataset.publisher.name, origin=ORIGIN_METADATA)
    sanction.add("sourceUrl", dataset.url, origin=ORIGIN_METADATA)
    sanction.set("program", program_name)

    if program_key is not None:
        program = programs.get_program_by_key(program_key)
        if program:
            sanction.set(
                "programId",
                program_key,
                original_value=source_program_key,
                origin=ORIGIN_METADATA,
            )
            entity.add("programId", program_key, origin=ORIGIN_METADATA)
            sanction.add("programUrl", program.url, origin=ORIGIN_METADATA)
        else:
            context.log.warn(
                f"Program with key {program_key!r} not found.",
                entity_id=entity.id,
            )

    if start_date:
        h.apply_date(sanction, "startDate", start_date)
    if end_date:
        h.apply_date(sanction, "endDate", end_date)
        if not sanction.get("endDate"):
            raise ValueError(
                f"Sanction end_date {end_date!r} could not be parsed as a date "
                f"(entity {entity.id!r}). Add a datepatterns entry or a lookup "
                "to clean the value."
            )
        sanction.add("status", "active" if is_active(sanction) else "inactive")

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
        iso_start_date is None or not starts_after(iso_start_date, settings.RUN_TIME)
    ) and (iso_end_date is None or not ended_before(iso_end_date, settings.RUN_TIME))
    return is_active
