from rigour.reset import reset_caches as reset_rigour_caches

from zavod.helpers.addresses import format_address


def reset_caches() -> None:
    format_address.cache_clear()
    reset_rigour_caches()
