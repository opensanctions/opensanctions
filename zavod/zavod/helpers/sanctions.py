from rigour.dates import ended_before, starts_after

from zavod import helpers as h
from zavod import settings
from zavod.constants import ORIGIN_METADATA, ORIGIN_OBSERVED
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
    # TODO: derive status only in finalize_sanction, once all sanctions crawlers
    # call it, so that dates applied after construction count towards it.
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


def finalize_sanction(
    context: Context,
    sanction: Entity,
    effective_on_listing: bool = False,
    amendments_dated: bool = False,
    key: str | None = None,
    checksum: str | None = None,
) -> None:
    """Complete a sanction's dates and status once the crawler has set them.

    Call this after all source dates are applied to the sanction and before
    emitting it. If the sanction has an ``endDate`` but no ``status``, the
    status is derived with `is_active`.

    Args:
        context: The runner context with dataset metadata.
        sanction: The Sanction to finalize.
        key: The authority-issued ID of the designation. It is recorded with
            `observe_item`, and a missing ``listingDate`` is filled with the
            date it was first seen.
        checksum: A checksum of the designation's source content, which needs a
            ``key``. It is recorded with `observe_change`, and a missing
            ``modifiedAt`` is filled with the date the content was first seen.
            Observations are always recorded, but only fill dates when
            ``settings.FEAT_FINALIZE_OBSERVE`` is set, and never for records
            already in the source at the dataset's first observation.
        effective_on_listing: Designations on this list take effect when they are
            listed, as set by the legal instrument. Check this per list. If one of
            ``listingDate`` and ``startDate`` is missing, it is filled with the
            earliest value of the other, keeping that value's origin.
        amendments_dated: The source dates every amendment of a designation, so
            one without a ``modifiedAt`` was never amended. Check this per list,
            e.g. against the archived source history. If ``modifiedAt`` is
            missing, it is filled with the latest ``listingDate``, or the
            latest ``startDate`` if there is no listing date.
    """
    assert sanction.schema.is_a("Sanction"), sanction.schema
    assert checksum is None or key is not None, "An observed checksum needs a key"
    if key is not None:
        item_seen = h.observe_item(context, key)
        change_seen = None
        if checksum is not None:
            change_seen = h.observe_change(context, key, checksum)
        if settings.FEAT_FINALIZE_OBSERVE:
            if item_seen is not None and not sanction.has("listingDate"):
                item_date = item_seen.date().isoformat()
                sanction.add("listingDate", item_date, origin=ORIGIN_OBSERVED)
            if change_seen is not None and not sanction.has("modifiedAt"):
                change_date = change_seen.date().isoformat()
                sanction.add("modifiedAt", change_date, origin=ORIGIN_OBSERVED)

    if effective_on_listing:
        listing = sanction.get_statements("listingDate")
        start = sanction.get_statements("startDate")
        if len(listing) > 0 and len(start) == 0:
            earliest = min(listing, key=lambda stmt: stmt.value)
            sanction.adopt_statement(earliest, prop="startDate")
        elif len(start) > 0 and len(listing) == 0:
            earliest = min(start, key=lambda stmt: stmt.value)
            sanction.adopt_statement(earliest, prop="listingDate")

    # A modifiedAt taken from the listing asserts the designation was never
    # amended, which is false on lists that amend without dating it (e.g. eu_fsf).
    # Of several candidate dates (source, observed), the latest one wins.
    if amendments_dated and not sanction.has("modifiedAt"):
        listed = sanction.get_statements("listingDate")
        if len(listed) == 0:
            listed = sanction.get_statements("startDate")
        if len(listed) > 0:
            latest = max(listed, key=lambda stmt: stmt.value)
            sanction.adopt_statement(latest, prop="modifiedAt")

    if sanction.has("endDate") and not sanction.has("status"):
        sanction.add("status", "active" if is_active(sanction) else "inactive")


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
