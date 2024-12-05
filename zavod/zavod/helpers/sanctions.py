from typing import Optional
from zavod.context import Context
from zavod.entity import Entity


def make_sanction(
    context: Context,
    entity: Entity,
    key: Optional[str] = None,
    program: Optional[str] = None,
) -> Entity:
    """Create and return a sanctions object derived from the dataset metadata.

    The country, authority, sourceUrl, and subject entity properties
    are automatically set.

    Args:
        context: The runner context with dataset metadata.
        entity: The entity to which the sanctions object will be linked.
        key: An optional key to be included in the ID of the sanction.
        program: An optional key for looking up the program ID in the YAML configuration.

    Returns:
        A new entity of type Sanction.
    """
    assert entity.schema.is_a("Thing"), entity.schema
    assert entity.id is not None, entity.id
    dataset = context.dataset
    assert dataset.publisher is not None
    sanction = context.make("Sanction")
    sanction.id = context.make_id("Sanction", entity.id, key)
    sanction.add("entity", entity)
    if dataset.publisher.country != "zz":
        sanction.add("country", dataset.publisher.country)
    sanction.add("authority", dataset.publisher.name)
    sanction.add("sourceUrl", dataset.url)
    sanction.add("program", program)

    if program is not None:
        program_id = context.lookup_value("sanction", program)
        if program_id is None:
            context.log.warn(f"Program key '{program}' not found.")
    return sanction
