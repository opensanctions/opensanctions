from rigour.ids import ISIN

from zavod.constants import ORIGIN_INFERRED
from zavod.context import Context
from zavod.entity import Entity
from zavod.logs import get_logger

log = get_logger(__name__)

ISIN_NON_COUNTRY = ("XS", "XD", "XC", "XF", "CS", "QS")


def make_security(context: Context, isin: str) -> Entity | None:
    """Make a security entity from an ISIN, or None if the value is not a valid ISIN.

    Args:
        context: The crawler context.
        isin: The ISIN to validate and normalize.

    Returns:
        A ``Security`` entity, or ``None`` if ``isin`` fails validation.
    """
    normalized = ISIN.normalize(isin)
    if normalized is None:
        log.warning("Invalid ISIN, skipping security", isin=isin)
        return None
    entity = context.make("Security")
    entity.id = f"isin-{normalized}"
    entity.add("isin", normalized)
    cc = normalized[:2]
    if cc not in ISIN_NON_COUNTRY:
        entity.add("country", cc, origin=ORIGIN_INFERRED)
    return entity
