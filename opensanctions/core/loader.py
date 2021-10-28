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
    def assemble(cls, types, props, resolver):
        entity = None
        for stmt in types:
            schema = model.get(stmt.schema)
            if entity is None:
                entity = Entity(schema)
                entity.id = str(stmt.canonical_id)
                entity.first_seen = stmt.first_seen
                entity.last_seen = stmt.last_seen
                entity.target = stmt.target
            else:
                entity.add_schema(schema)
                entity.first_seen = min(entity.first_seen, stmt.first_seen)
                entity.last_seen = max(entity.last_seen, stmt.last_seen)
                entity.target = max(entity.target, stmt.target)
            entity.datasets.add(Dataset.get(stmt.dataset))
            entity.referents.add(str(stmt.entity_id))

        for stmt in props:
            prop = entity.schema.properties[stmt.prop]
            value = str(stmt.value)
            if prop.type == registry.entity:
                value = resolver.get_canonical(value)
            entity.unsafe_add(prop, value, cleaned=True)
        return entity

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
        types = []
        props = []
        q = Statement.all_statements(
            dataset=dataset,
            canonical_id=canonical_id,
            inverted_ids=inverted_ids,
        )
        for stmt in q:
            if stmt.canonical_id != current_id:
                if len(types):
                    yield cls.assemble(types, props, resolver)
                types = []
                props = []
            current_id = stmt.canonical_id
            if stmt.prop == stmt.BASE:
                types.append(stmt)
            else:
                props.append(stmt)
        if len(types):
            yield cls.assemble(types, props, resolver)
