import structlog
from typing import List
from opensanctions.core.lookups import common_lookups

log = structlog.get_logger(__name__)


def clean_gender(value: str) -> List[str]:
    """Not clear if this function name is offensive or just weird."""
    lookup = common_lookups().get("gender")
    if lookup is None or value is None:
        return [value]
    results = lookup.get_values(value, default=[])
    if not len(results):
        log.warning("Gender not mapped", gender=value)
        return [value]
    return results
