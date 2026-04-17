from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

POSITION_LABELS: set[str] = {"Bourgmestre", "Échevin"}


def crawl_record(
    context: Context,
    record: dict[str, Any],
    positions: dict[str, tuple[Entity, PositionCategorisation]],
) -> None:
    com_code = record.pop("COM_CODE")
    com_label = record.pop("COM_LABEL")
    man_label = record.pop("MAN_LABEL")
    first_name = record.pop("ELU_FIRST_NAME")
    last_name = record.pop("ELU_LAST_NAME")
    gender = record.pop("ELU_SEX")
    start_date = record.pop("COAC_START_DATE")
    end_date = record.pop("COAC_END_DATE")

    assert man_label in POSITION_LABELS, f"Unknown position: {man_label!r}"

    pos_key = f"{man_label}-{com_code}"
    if pos_key not in positions:
        position = h.make_position(
            context,
            name=f"{man_label} de {com_label}",
            country="lu",
            subnational_area=com_label,
            lang="fra",
        )
        cat = categorise(context, position, is_pep=True)
        context.emit(position)
        positions[pos_key] = (position, cat)

    position, cat = positions[pos_key]

    person = context.make("Person")
    person.id = context.make_id(first_name, last_name, com_code)
    h.apply_name(person, first_name=first_name, last_name=last_name, lang="fra")
    person.add("gender", gender)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=cat,
        start_date=start_date,
        end_date=end_date or None,
        propagate_country=True,
    )
    if occupancy is not None:
        context.emit(occupancy)
        context.emit(person)

    context.audit_data(record)


def crawl(context: Context) -> None:
    data = context.fetch_json(context.data_url, cache_days=1)
    assert isinstance(data, list), f"Expected list, got {type(data)}"

    positions: dict[str, tuple[Entity, PositionCategorisation]] = {}
    for record in data:
        crawl_record(context, record, positions)
