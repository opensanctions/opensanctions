from rigour.reset import reset_caches as reset_rigour_caches

from zavod.helpers.addresses import format_address
from zavod.logic.pep import categorise


def reset_caches() -> None:
    reset_rigour_caches()
    format_address.cache_clear()
    categorise.cache_clear()
