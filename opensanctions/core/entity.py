import structlog
from banal import ensure_list

from followthemoney import model
from followthemoney.types import registry
from followthemoney.proxy import EntityProxy

from opensanctions.model import Statement
from opensanctions.helpers import type_lookup

log = structlog.get_logger(__name__)


class Entity(EntityProxy):
    """Entity for sanctions list entries and adjacent objects.

    Add utility methods to the :py:class:`followthemoney.proxy:EntityProxy` for
    extracting data from sanctions lists and for auditing parsing errors to
    structured logging.
    """

    def __init__(self, dataset, schema, data=None, target=False):
        self.dataset = dataset
        self.sources = set()
        self.target = target
        self.first_seen = None
        self.last_seen = None
        data = data or {"schema": schema}
        super().__init__(model, data, key_prefix=dataset.name)

    def make_slug(self, *parts, strict=True):
        self.id = self.dataset.make_slug(*parts, strict=strict)
        return self.id

    def make_id(self, *parts):
        hashed = super().make_id(*parts)
        return self.make_slug(hashed)

    def _lookup_values(self, prop, values):
        for value in ensure_list(values):
            yield from type_lookup(prop.type, value)

    def add(self, prop, values, cleaned=False, quiet=False, fuzzy=False):
        if cleaned:
            super().add(prop, values, cleaned=True, quiet=quiet, fuzzy=fuzzy)
            return

        prop_name = self._prop_name(prop, quiet=quiet)
        if prop_name is None:
            return
        prop = self.schema.properties[prop_name]

        for value in self._lookup_values(prop, values):
            if value is None or len(str(value).strip()) == 0:
                continue
            clean = prop.type.clean(value, proxy=self, fuzzy=fuzzy)
            if clean is None:
                if prop.type == registry.phone:
                    continue
                log.warning(
                    "Rejected property value",
                    entity=self,
                    prop=prop.name,
                    value=value,
                )
            super().add(prop, clean, cleaned=True, quiet=False, fuzzy=fuzzy)

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
        self.add_schema(schema)
        return self.add(prop, value)

    def add_schema(self, schema):
        """Try to apply the given schema to the current entity, making it more
        specific (e.g. turning a `LegalEntity` into a `Company`). This raises an
        exception if the current and new type are incompatible."""
        self.schema = model.common_schema(self.schema, schema)

    def to_dict(self):
        data = super().to_dict()
        data["first_seen"] = self.first_seen
        data["last_seen"] = self.last_seen
        data["target"] = self.target
        data["dataset"] = self.dataset.name
        data["caption"] = self.caption
        return data

    @classmethod
    def query(cls, dataset, entity_id=None, inverted_id=None):
        """Query the statement table for the given dataset and entity ID and return
        re-constructed entities with the given properties."""
        current_entity_id = None
        entity = None
        for stmt in Statement.all_statements(
            dataset=dataset, entity_id=entity_id, inverted_id=inverted_id
        ):
            schema = model.get(stmt.schema)
            if stmt.entity_id != current_entity_id:
                if entity is not None:
                    yield entity
                entity = cls(dataset, schema)
                entity.id = stmt.entity_id
                entity.first_seen = stmt.first_seen
                entity.last_seen = stmt.last_seen
            current_entity_id = stmt.entity_id
            entity.add_schema(schema)
            entity.sources.add(stmt.dataset)
            entity.add(stmt.prop, stmt.value, cleaned=True)
            entity.target = max(entity.target, stmt.target)
            entity.first_seen = min(entity.first_seen, stmt.first_seen)
            entity.last_seen = max(entity.last_seen, stmt.last_seen)
        if entity is not None:
            yield entity

    # TODO: from_dict!!
