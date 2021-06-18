import structlog
from banal import ensure_list

from followthemoney import model
from followthemoney.proxy import EntityProxy

from opensanctions.model import db, Statement

log = structlog.get_logger(__name__)


class Entity(EntityProxy):
    """Add utility methods to the entity proxy for extracting data from sanctions
    lists."""

    def __init__(self, dataset, schema, data=None):
        self.dataset = dataset
        data = data or {"schema": schema}
        super().__init__(model, data, key_prefix=dataset.name)

    def make_slug(self, *parts, strict=True):
        self.id = self.dataset.make_slug(*parts, strict=strict)
        return self.id

    def _lookup_values(self, prop, values):
        values = ensure_list(values)
        lookup = self.dataset.lookups.get(prop.type.name)
        if lookup is None:
            yield from values
            return

        for value in values:
            yield from lookup.get_values(value, value)

    def add(self, prop, values, cleaned=False, quiet=False, fuzzy=False):
        prop_name = self._prop_name(prop, quiet=quiet)
        if prop_name is None:
            return
        prop = self.schema.properties[prop_name]

        for value in self._lookup_values(prop, values):
            if value is None or len(str(value).strip()) == 0:
                continue
            if not cleaned:
                raw = value
                value = prop.type.clean(value, proxy=self, fuzzy=fuzzy)
            if value is None:
                log.warning(
                    "Rejected property value",
                    entity=self,
                    prop=prop.name,
                    value=raw,
                )
            super().add(prop, value, cleaned=True, quiet=False, fuzzy=fuzzy)

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

    def add_address(
        self,
        full=None,
        remarks=None,
        postOfficeBox=None,
        street=None,
        street2=None,
        city=None,
        postalCode=None,
        region=None,
        latitude=None,
        longitude=None,
        country=None,
    ):
        assert self.schema.is_a("Thing"), self.schema
        if full is not None:
            self.add("address", full)

    @classmethod
    def query(cls, dataset, entity_id=None):
        current_entity_id = None
        entity = None
        for stmt in Statement.all_statements(dataset=dataset, entity_id=entity_id):
            schema = model.get(stmt.schema)
            if stmt.entity_id != current_entity_id:
                if entity is not None:
                    yield entity
                entity = cls(dataset, schema)
                entity.id = stmt.entity_id
            current_entity_id = stmt.entity_id
            entity.schema = model.common_schema(entity.schema, schema)
            entity.add(stmt.prop, stmt.value, cleaned=True)
        if entity is not None:
            yield entity

    # TODO: from_dict!!
