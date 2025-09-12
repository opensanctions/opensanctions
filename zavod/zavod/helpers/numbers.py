from decimal import Decimal
from typing import Optional, Union
from followthemoney import registry
from rigour.units import normalize_unit

from zavod.logs import get_logger
from zavod.entity import Entity

NumberValue = Union[str, int, float, Decimal]
log = get_logger(__name__)


def apply_number(
    entity: Entity, prop: str, value: NumberValue, origin: Optional[str] = None
) -> None:
    """Apply a numeric value to a property of an entity. This will try and parse the
    number, round it, and normalize the present unit specifier (e.g. km, tons) if present.

    Args:
        entity: The entity to which the property belongs.
        prop: The property to which the value will be applied.
        value: The numeric value to apply.
        origin: An optional origin for the value.
    """
    prop_obj = entity.schema.get(prop)
    assert prop_obj and prop_obj.type == registry.number
    if isinstance(value, str):
        if not len(value.strip()):
            return
        num, unit = registry.number.parse(
            value,
            decimal=entity.dataset.numbers.decimal,
            separator=entity.dataset.numbers.separator,
        )
        if num is None:
            log.warning("Cannot parse number: %s", value)
            return
        if unit is not None:
            unit = normalize_unit(unit)
        if unit is not None:
            text = f"{num} {unit}"
        else:
            text = num
    elif isinstance(value, (float, Decimal)):
        text = f"{value:.2f}"
    else:
        text = str(value)
    entity.unsafe_add(
        prop_obj,
        text,
        cleaned=True,
        original_value=str(value),
        origin=origin,
    )
