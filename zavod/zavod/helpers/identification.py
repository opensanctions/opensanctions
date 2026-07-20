from typing import Optional

from zavod.logs import get_logger
from zavod.entity import Entity
from zavod.context import Context
from zavod.helpers.dates import apply_date

log = get_logger(__name__)


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
    origin: Optional[str] = None,
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
        origin: An optional origin to attribute the emitted statements to, e.g.
            the model behind a reviewed extraction.

    Returns:
        A new entity of type `Identification` or `Passport`.
    """
    schema = "Passport" if passport else "Identification"
    proxy = context.make(schema)
    holder_prop = proxy.schema.get("holder")
    assert holder_prop is not None
    assert holder_prop.range is not None
    if not entity.schema.is_a(holder_prop.range):
        log.warning(
            "Holder is not a valid type for %s" % schema,
            entity_schema=entity.schema,
            entity_id=entity.id,
            number=number,
        )
        return None

    if number is None:
        return None
    # It is very unlikely that two countries issue the same person a document
    # with the same number.
    proxy.id = context.make_id(entity.id, number, doc_type, key)
    proxy.add("holder", entity.id, origin=origin)
    proxy.add("number", number, origin=origin)
    proxy.add("type", doc_type, origin=origin)
    proxy.add("country", country, origin=origin)
    proxy.add("authority", authority, origin=origin)
    proxy.add("summary", summary, origin=origin)
    apply_date(proxy, "startDate", start_date)
    apply_date(proxy, "endDate", end_date)
    # context.inspect(proxy.to_dict())
    if passport:
        entity.add("passportNumber", number, origin=origin)
    else:
        entity.add("idNumber", number, origin=origin)
    return proxy
