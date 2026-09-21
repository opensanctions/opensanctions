from rigour.ids import ISIN

from zavod.constants import ORIGIN_INFERRED
from zavod.context import Context
from zavod.entity import Entity

ISIN_NON_COUNTRY = ("XS", "XD", "XC", "XF", "CS", "QS")


def make_security(context: Context, isin: str) -> Entity | None:
    """Make a security entity keyed on its ISIN.

    The entity ID is minted in the global ``isin-`` namespace, which is shared
    across datasets, so the ISIN is validated (checksum) and normalized
    (whitespace and case) before it is used. Only a validated ISIN is used to
    infer the security's country.

    Args:
        context: The runner context.
        isin: The ISIN as it appears in the source.

    Returns:
        A new entity of type ``Security``, or ``None`` if the value is not a
        valid ISIN. A warning is logged in that case.
    """
    normalized = ISIN.normalize(isin)
    if normalized is None:
        context.log.warning("Invalid ISIN", isin=isin)
        return None
    entity = context.make("Security")
    entity.id = f"isin-{normalized}"
    entity.add("isin", normalized)
    cc = normalized[:2]
    if cc not in ISIN_NON_COUNTRY:
        entity.add("country", cc, origin=ORIGIN_INFERRED)
    return entity
