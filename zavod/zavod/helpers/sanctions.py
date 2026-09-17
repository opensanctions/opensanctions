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


def _apply_program_and_dates(
    context: Context,
    entity: Entity,
    interval: Entity,
    source_program_key: str | None,
    program_key: str | None,
    start_date: str | None,
    end_date: str | None,
) -> None:
    """Resolve the program key onto an interval and apply its date range.

    Shared by `make_sanction` and `make_risk`, which differ only in the schema
    they construct.

    Args:
        context: The runner context with dataset metadata.
        entity: The entity the interval is linked to.
        interval: The Sanction or Risk to apply the program and dates to.
        source_program_key: Program key at the source, will be set as the original value for programId.
        program_key: An optional OpenSanction program key.
        start_date: An optional start date for the interval.
        end_date: An optional end date for the interval.
    """
    if program_key is not None:
        program = programs.get_program_by_key(program_key)
        if program:
            interval.set(
                "programId",
                program_key,
                original_value=source_program_key,
                origin=ORIGIN_METADATA,
            )
            entity.add("programId", program_key, origin=ORIGIN_METADATA)
            interval.add("programUrl", program.url, origin=ORIGIN_METADATA)
        else:
            context.log.warn(
                f"Program with key {program_key!r} not found.",
                entity_id=entity.id,
            )

    if start_date:
        h.apply_date(interval, "startDate", start_date)
    if end_date:
        h.apply_date(interval, "endDate", end_date)
        if not interval.get("endDate"):
            raise ValueError(
                f"{interval.schema.name} end_date {end_date!r} could not be parsed "
                f"as a date (entity {entity.id!r}). Add a datepatterns entry or a "
                "lookup to clean the value."
            )
        interval.add("status", "active" if is_active(interval) else "inactive")


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
    _apply_program_and_dates(
        context,
        entity,
        sanction,
        source_program_key,
        program_key,
        start_date,
        end_date,
    )
    return sanction


def make_risk(
    context: Context,
    entity: Entity,
    key: str | None = None,
    program_name: str | None = None,
    source_program_key: str | None = None,
    program_key: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> Entity:
    """Create and return a risk object derived from the dataset metadata.

    A Risk records that a source named an entity under some program without
    imposing a legal restriction on it: an advisory or watch list, a register
    of persons of interest, or a report that identifies people without
    designating them. Where the source does impose a restriction, use
    `make_sanction` instead.

    The country, authority, sourceUrl, and subject entity properties
    are automatically set.

    If an ``end_date`` is given, a ``status`` of "active" or "inactive" is
    derived using the same semantics as `is_active`. Note that the status is
    only computed at construction time: dates applied to the risk afterwards
    (e.g. via `h.apply_date`) do not update it.

    Args:
        context: The runner context with dataset metadata.
        entity: The entity to which the risk object will be linked.
        key: An optional key to be included in the ID of the risk.
        program_name: An optional program name.
        program_key: An optional OpenSanction program key.
        source_program_key: Program key at the source, will be set as the original value for programId.
        start_date: An optional start date for the risk.
        end_date: An optional end date for the risk.

    Returns:
        A new entity of type Risk.
    """
    assert entity.schema.is_a("Thing"), entity.schema
    assert entity.id is not None, entity.id
    dataset = context.dataset.model
    assert dataset.publisher is not None
    risk = context.make("Risk")
    risk.id = context.make_id("Risk", entity.id, key)
    risk.add("entity", entity)
    if dataset.publisher.country != "zz":
        risk.add("country", dataset.publisher.country, origin=ORIGIN_METADATA)
    risk.add("authority", dataset.publisher.name, origin=ORIGIN_METADATA)
    risk.add("sourceUrl", dataset.url, origin=ORIGIN_METADATA)
    risk.set("program", program_name)
    _apply_program_and_dates(
        context,
        entity,
        risk,
        source_program_key,
        program_key,
        start_date,
        end_date,
    )
    return risk


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
