def make_sanction(context, entity, key=None):
    """Create a sanctions object derived from the dataset metadata."""
    assert entity.schema.is_a("Thing"), entity.schema
    dataset = context.dataset
    sanction = context.make("Sanction")
    sanction.id = context.make_id("Sanction", entity.id, key)
    sanction.add("entity", entity)
    if dataset.publisher.country != "zz":
        sanction.add("country", dataset.publisher.country)
    sanction.add("authority", dataset.publisher.name)
    sanction.add("sourceUrl", dataset.url)
    return sanction
