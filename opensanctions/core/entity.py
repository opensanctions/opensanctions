import structlog

from followthemoney import model
from followthemoney.proxy import EntityProxy
from followthemoney.util import value_list

log = structlog.get_logger(__name__)


class OSETLEntity(EntityProxy):
    """Add utility methods to the entity proxy for extracting data from sanctions
    lists."""

    def __init__(self, dataset, schema, data=None):
        self.dataset = dataset
        data = data or {"schema": schema}
        super().__init__(model, data, key_prefix=dataset.name)

    def make_slug(self, *parts, strict=True):
        self.id = self.dataset.make_slug(*parts, strict=strict)
        return self.id

    def add(self, prop, values, cleaned=False, quiet=False, fuzzy=False):
        prop_name = self._prop_name(prop, quiet=quiet)
        if prop_name is None:
            return
        prop = self.schema.properties[prop_name]

        for value in value_list(values):
            if value is None or len(str(value).strip()) == 0:
                continue
            if not cleaned:
                raw = value
                value = prop.type.clean(value, proxy=self, fuzzy=fuzzy)
            if value is None:
                log.warning(
                    "Rejected property value",
                    entity=self.id,
                    schema=self.schema.name,
                    prop=prop.name,
                    value=raw,
                )
            super().add(prop, value, cleaned=True, quiet=quiet, fuzzy=fuzzy)

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

    # TODO: from_dict!!
