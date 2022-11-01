from typing import Any, List
import yaml
from functools import lru_cache
from datapatch import get_lookups
from followthemoney.types.common import PropertyType

from opensanctions import settings


def load_yaml(file_path):
    """Safely parse a YAML document."""
    with open(file_path, "r", encoding=settings.ENCODING) as fh:
        return yaml.load(fh, Loader=yaml.SafeLoader)


@lru_cache(maxsize=None)
def common_lookups():
    common_path = settings.STATIC_PATH.joinpath("common.yml")
    return get_lookups(load_yaml(common_path))


@lru_cache(maxsize=None)
def type_lookup(type_: PropertyType, value: str) -> List[Any]:
    """Given a value and a certain property type, check to see if there is a
    normalised override available. This uses the lookups defined in
    `common.yml`.

    The override value is then cleaned again and applied to the entity."""
    lookup = common_lookups().get(type_.name)
    if lookup is None:
        return [value]
    return lookup.get_values(value, default=[value])
