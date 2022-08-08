from typing import Optional
from opensanctions.core.context import Context
from opensanctions.core.entity import Entity


def make_identification(
    context: Context,
    entity: Entity,
    number: Optional[str],
    doc_type: Optional[str] = None,
    country: Optional[str] = None,
    summary: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    authority: Optional[str] = None,
    key: Optional[str] = None,
    passport: bool = False,
) -> Optional[Entity]:
    """Create an `Identification` or `Passport` object."""
    schema = "Passport" if passport else "Identification"
    proxy = context.make(schema)
    if number is None:
        return None
    proxy.id = context.make_id(entity.id, number, doc_type, key)
    proxy.add("holder", entity)
    proxy.add("number", number)
    proxy.add("type", doc_type)
    proxy.add("country", country)
    proxy.add("authority", authority)
    proxy.add("summary", summary)
    proxy.add("startDate", start_date)
    proxy.add("endDate", end_date)
    # context.inspect(proxy.to_dict())
    return proxy
