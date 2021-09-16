import structlog
from typing import Generator, Iterator, Optional, Tuple
from followthemoney import model
from followthemoney.types import registry
from followthemoney.property import Property
from nomenklatura import Loader, MemoryLoader, Resolver


from opensanctions.core.dataset import Dataset
from opensanctions.core.entity import Entity
from opensanctions.model import Statement

log = structlog.get_logger(__name__)


class DatasetMemoryLoader(MemoryLoader[Dataset, Entity]):
    def __init__(self, dataset: Dataset, resolver: Resolver):
        entities = DatabaseLoader.query(resolver, dataset)
        super().__init__(dataset, entities, resolver=resolver)


class DatabaseLoader(Loader[Dataset, Entity]):
    def __init__(self, dataset: Dataset, resolver: Resolver):
        super().__init__(dataset)
        self.resolver = resolver

    def get_entity(self, id: str) -> Optional[Entity]:
        for entity in self.query(self.resolver, self.dataset, entity_id=id):
            return entity
        return None

    def get_inverted(self, id: str) -> Generator[Tuple[Property, Entity], None, None]:
        for entity in self.query(self.resolver, self.dataset, inverted_id=id):
            for prop, value in entity.itervalues():
                if prop.type == registry.entity and value == id:
                    yield prop.reverse, entity

    def __iter__(self) -> Iterator[Entity]:
        return iter(self.query(self.resolver, self.dataset))

    def __len__(self) -> int:
        return Statement.all_ids(self.dataset).count()

    def __repr__(self):
        return f"<DatabaseLoader({self.dataset!r})>"

    @classmethod
    def query(cls, resolver, dataset, entity_id=None, inverted_id=None):
        """Query the statement table for the given dataset and entity ID and return
        re-constructed entities with the given properties."""
        canonical_id = None
        if entity_id is not None:
            canonical_id = resolver.get_canonical(entity_id)
        inverted_ids = None
        if inverted_id is not None:
            inverted_ids = resolver.get_referents(inverted_id)
        current_id = None
        entity = None
        q = Statement.all_statements(
            dataset=dataset,
            canonical_id=canonical_id,
            inverted_ids=inverted_ids,
        )
        for stmt in q:
            schema = model.get(stmt.schema)
            if stmt.canonical_id != current_id:
                if entity is not None:
                    yield entity
                entity = Entity(dataset, schema)
                entity.id = stmt.canonical_id
                entity.first_seen = stmt.first_seen
                entity.last_seen = stmt.last_seen
            current_id = stmt.canonical_id
            entity.add_schema(schema)
            entity.sources.add(stmt.dataset)
            prop = schema.properties[stmt.prop]
            value = stmt.value
            if prop.type == registry.entity:
                value = resolver.get_canonical(value)
            entity.unsafe_add(prop, value, cleaned=True)
            entity.target = max(entity.target, stmt.target)
            entity.first_seen = min(entity.first_seen, stmt.first_seen)
            entity.last_seen = max(entity.last_seen, stmt.last_seen)
        if entity is not None:
            yield entity
