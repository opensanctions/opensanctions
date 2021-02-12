import structlog

from followthemoney import model
from followthemoney.proxy import EntityProxy

log = structlog.get_logger(__name__)


class OSETLEntity(EntityProxy):
    """Add utility methods to the entity proxy for extracting data from sanctions
    lists."""

    def __init__(self, dataset, schema, data=None):
        self.dataset = dataset
        data = data or {"schema": schema}
        super().__init__(model, data, key_prefix=dataset.name)

    def add_cast(self, schema, prop, value):
        """Set a property on an entity. If the entity is of a schema that doesn't
        have the given property, also modify the schema (e.g. if something has a
        birthDate, assume it's a Person, not a LegalEntity).
        """
        if self.schema.get(prop) is not None:
            return self.add(prop, value)

        schema = model.get(schema)
        prop_ = schema.get(prop)
        if prop_.type.clean(value) is None:
            return
        self.schema = model.common_schema(self.schema, schema)
        return self.add(prop, value)
