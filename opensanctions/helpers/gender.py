from typing import List
from opensanctions.helpers.lookups import common_lookups


def clean_gender(value: str) -> List[str]:
    """Not clear if this function name is offensive or just weird."""
    lookup = common_lookups().get("gender")
    if lookup is None:
        return [value]
    return lookup.get_values(value, default=[value])
