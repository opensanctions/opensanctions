from typing import Optional

from zavod.entity import Entity
from zavod.context import Context


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
    """Create an `Identification` or `Passport` object linked to a passport holder.

    Args:
        context: The context used for making entities.
        entity: The entity that holds the passport.
        number: The passport number.
        doc_type: The type of document (e.g. "passport", "national id").
        country: The country that issued the passport.
        summary: A summary of the passport details.
        start_date: The date the passport was issued.
        end_date: The date the passport expires.
        authority: The issuing authority.
        key: An optional key to be included in the ID of the identification.
        passport: Whether the identification is a passport or not.

    Returns:
        A new entity of type `Identification` or `Passport`.
    """
    schema = "Passport" if passport else "Identification"
    proxy = context.make(schema)
    holder_prop = proxy.schema.get("holder")
    assert holder_prop is not None
    assert holder_prop.range is not None
    assert entity.schema.is_a(holder_prop.range)

    if number is None:
        return None
    proxy.id = context.make_id(entity.id, number, doc_type, key)
    proxy.add("holder", entity.id)
    proxy.add("number", number)
    proxy.add("type", doc_type)
    proxy.add("country", country)
    proxy.add("authority", authority)
    proxy.add("summary", summary)
    proxy.add("startDate", start_date)
    proxy.add("endDate", end_date)
    # context.inspect(proxy.to_dict())
    if passport:
        entity.add("passportNumber", number)
    else:
        entity.add("idNumber", number)
    return proxy
