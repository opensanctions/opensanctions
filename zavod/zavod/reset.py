from rigour.reset import reset_caches as reset_rigour_caches

from zavod.helpers.addresses import format_address
from zavod.logic.pep import cached_cat_library


def reset_caches() -> None:
    reset_rigour_caches()
    format_address.cache_clear()
    cached_cat_library.cache_clear()
