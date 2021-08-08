def make_sanction(entity, key=None):
    """Create a sanctions object derived from the dataset metadata."""
    dataset = entity.dataset
    assert entity.schema.is_a("Thing"), entity.schema
    sanction = dataset.make_entity("Sanction")
    sanction.make_id("Sanction", entity.id, key)
    sanction.add("entity", entity)
    if dataset.publisher.country != "zz":
        sanction.add("country", dataset.publisher.country)
    sanction.add("authority", dataset.publisher.name)
    sanction.add("sourceUrl", dataset.url)
    return sanction
