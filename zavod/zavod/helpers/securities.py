from zavod.context import Context
from zavod.entity import Entity

ISIN_NON_COUNTRY = ("XS", "XD", "XC", "XF", "CS", "QS")


def make_security(context: Context, isin: str) -> Entity:
    """Make a security entity."""
    isin = isin.upper()
    entity = context.make("Security")
    entity.id = f"isin-{isin}"
    entity.add("isin", isin)
    cc = isin[:2]
    if cc not in ISIN_NON_COUNTRY:
        entity.add("country", cc)
    return entity
