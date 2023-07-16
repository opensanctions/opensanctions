from typing import List, Optional
from followthemoney.types.common import PropertyType

from zavod.meta.dataset import Dataset


def type_lookup(
    dataset: Dataset, type_: PropertyType, value: Optional[str]
) -> List[str]:
    """Given a value and a certain property type, check to see if there is a
    normalised override available. This uses the lookups defined in the dataset
    metadata. If no override is available, the value is returned as-is."""
    lookup = dataset.lookups.get(f"type.{type_.name}")
    default = [] if value is None else [value]
    if lookup is None:
        return default
    return lookup.get_values(value, default=default)
