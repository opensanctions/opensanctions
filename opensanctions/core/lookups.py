import yaml
from typing import TYPE_CHECKING, List, Optional
from functools import lru_cache
from datapatch import get_lookups
from followthemoney.types.common import PropertyType

from opensanctions import settings

if TYPE_CHECKING:
    from opensanctions.core.dataset import Dataset


def load_yaml(file_path):
    """Safely parse a YAML document."""
    with open(file_path, "r", encoding=settings.ENCODING) as fh:
        return yaml.load(fh, Loader=yaml.SafeLoader)


@lru_cache(maxsize=None)
def common_lookups():
    common_path = settings.STATIC_PATH.joinpath("common.yml")
    return get_lookups(load_yaml(common_path))


@lru_cache(maxsize=20000)
def _type_lookup(
    dataset: Optional["Dataset"], type_: PropertyType, value: Optional[str]
) -> List[str]:
    if dataset is not None:
        lookup = dataset.lookups.get(f"type.{type_.name}")
    if lookup is None:
        lookup = common_lookups().get(f"type.{type_.name}")
    if lookup is None:
        return [] if value is None else [value]
    return lookup.get_values(value, default=[value])


def type_lookup(
    dataset: Optional["Dataset"], type_: PropertyType, value: Optional[str]
) -> List[str]:
    """Given a value and a certain property type, check to see if there is a
    normalised override available. This uses the lookups defined in
    `common.yml`.

    The override value is then cleaned again and applied to the entity."""
    if not isinstance(value, (type(None), str)):
        return [value]
    return _type_lookup(dataset, type_, value)
