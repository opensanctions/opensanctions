from typing import TYPE_CHECKING, List, Optional, Tuple
from datapatch.lookup import Lookup
from followthemoney.property import Property
from followthemoney.types.common import PropertyType

from zavod.logs import get_logger
from zavod.meta.dataset import Dataset

if TYPE_CHECKING:
    from zavod.entity import Entity

log = get_logger(__name__)


def get_type_lookup(dataset: Dataset, type_: PropertyType) -> Optional[Lookup]:
    """Get a type-based lookup from the dataset by name."""
    return dataset.lookups.get(f"type.{type_.name}")


def type_lookup(
    dataset: Dataset, type_: PropertyType, value: Optional[str]
) -> List[str]:
    """Given a value and a certain property type, check to see if there is a
    normalised override available. This uses the lookups defined in the dataset
    metadata. If no override is available, the value is returned as-is."""
    lookup = get_type_lookup(dataset, type_)
    default = [] if value is None else [value]
    if lookup is None:
        return default
    return lookup.get_values(value, default=default)


def prop_lookup(
    entity: "Entity", prop: Property, value: Optional[str]
) -> List[Tuple[Property, str]]:
    """Given a value and a certain property, check to see if there is a type-based
    normalised override available. This uses the lookups defined in the dataset
    metadata. If no override is available, the value is returned as-is."""
    lookup = get_type_lookup(entity.dataset, prop.type)
    values = [] if value is None else [value]
    if lookup is not None:
        result = lookup.match(value)
        if result is not None:
            values = result.values

            # Check if the value is supposed to moved to a different property:
            if result.prop is not None:
                prop_ = entity.schema.get(result.prop)
                if prop_ is None:
                    log.warning(
                        "Invalid type lookup property re-write",
                        original=prop.name,
                        prop=result.prop,
                        schema=entity.schema.name,
                        value=value,
                    )
                else:
                    prop = prop_
                    # This is intended to allow mapping multiple values with one
                    # block to a new property, by leaving the value/values empty:
                    if value is not None and not len(values):
                        values = [value]
    return [(prop, v) for v in values]
