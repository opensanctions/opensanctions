import structlog
from typing import Callable, Dict, Generator, Iterator, List, Optional, Set, Tuple
from followthemoney import model
from followthemoney.types import registry
from followthemoney.property import Property
from nomenklatura import Loader, Resolver

from opensanctions.core.dataset import Dataset
from opensanctions.core.entity import Entity
from opensanctions.model import Statement

log = structlog.get_logger(__name__)


class CachedType(object):
    __slots__ = (
        "entity_id",
        "canonical_id",
        "schema",
        "target",
        "first_seen",
        "last_seen",
        "dataset",
    )

    def __init__(self, stmt: Statement):
        self.canonical_id = str(stmt.canonical_id)
        dataset = Dataset.get(stmt.dataset)
        if dataset is None:
            raise RuntimeError("Missing dataset: %r" % stmt.dataset)
        self.dataset = dataset
        self.schema = model.schemata[stmt.schema]
        self.entity_id = str(stmt.entity_id)
        self.first_seen = stmt.first_seen
        self.last_seen = stmt.last_seen
        self.target = stmt.target


class CachedProp(object):
    __slots__ = ("canonical_id", "value", "prop", "dataset")

    def __init__(self, stmt: Statement):
        self.canonical_id = str(stmt.canonical_id)
        dataset = Dataset.get(stmt.dataset)
        if dataset is None:
            raise RuntimeError("Missing dataset: %r" % stmt.dataset)
        self.dataset = dataset
        schema = model.schemata[stmt.schema]
        self.prop = schema.properties[stmt.prop]
        self.value = str(stmt.value)


CachedEntity = Tuple[Tuple[CachedType, ...], Tuple[CachedProp, ...]]
Assembler = Optional[Callable[[Entity], Entity]]


class Database(object):
    def __init__(
        self, scope: Dataset, resolver: Resolver[Entity], cached: bool = False
    ):
        self.scope = scope
        self.cached = cached
        self.resolver = resolver
        self.entities: Dict[str, CachedEntity] = {}
        self.inverted: Dict[str, Set[str]] = {}
        self.load()

    def view(self, dataset: Dataset, assembler: Assembler) -> "DatasetLoader":
        if self.cached:
            return CachedDatasetLoader(self, dataset, assembler)
        return DatasetLoader(self, dataset, assembler)

    def load(self) -> None:
        if not self.cached:
            return
        log.info("Loading database cache...", scope=self.scope)
        for cached in self.query(self.scope):
            entity_id = cached[0][0].canonical_id
            self.entities[entity_id] = cached
            for stmt in cached[1]:
                if stmt.prop.type != registry.entity:
                    continue
                value = stmt.value = self.resolver.get_canonical(stmt.value)
                if value not in self.inverted:
                    self.inverted[value] = set()
                self.inverted[value].add(entity_id)

    def query(
        self, dataset: Dataset, entity_id=None, inverted_id=None
    ) -> Generator[CachedEntity, None, None]:
        """Query the statement table for the given dataset and entity ID and return
        re-constructed entities with the given properties."""
        canonical_id = None
        if entity_id is not None:
            canonical_id = self.resolver.get_canonical(entity_id)
        inverted_ids = None
        if inverted_id is not None:
            inverted_ids = self.resolver.get_referents(inverted_id)
        current_id = None
        types: List[CachedType] = []
        props: List[CachedProp] = []
        q = Statement.all_statements(
            dataset=dataset,
            canonical_id=canonical_id,
            inverted_ids=inverted_ids,
        )
        for stmt in q:
            if stmt.canonical_id != current_id:
                if len(types):
                    yield (tuple(types), tuple(props))
                types = []
                props = []
            current_id = stmt.canonical_id
            if stmt.prop == Statement.BASE:
                types.append(CachedType(stmt))
            else:
                props.append(CachedProp(stmt))
        if len(types):
            yield (tuple(types), tuple(props))

    def assemble(self, cached: CachedEntity, sources=Optional[Set[Dataset]]):
        entity = None
        for stmt in cached[0]:
            if sources is not None and stmt.dataset not in sources:
                continue
            if entity is None:
                entity = Entity(stmt.schema)
                entity.id = stmt.canonical_id
                entity.first_seen = stmt.first_seen
                entity.last_seen = stmt.last_seen
                entity.target = stmt.target
            else:
                entity.add_schema(stmt.schema)
                entity.first_seen = min(entity.first_seen, stmt.first_seen)
                entity.last_seen = max(entity.last_seen, stmt.last_seen)
                entity.target = max(entity.target, stmt.target)
            entity.datasets.add(stmt.dataset)
            entity.referents.add(stmt.entity_id)

        if entity is None:
            return None

        for prop in cached[1]:
            if sources is not None and prop.dataset not in sources:
                continue
            entity.unsafe_add(prop.prop, prop.value, cleaned=True)
        return entity


class DatasetLoader(Loader[Dataset, Entity]):
    def __init__(self, database: Database, dataset: Dataset, assembler: Assembler):
        self.db = database
        self.dataset = dataset
        self.assembler = assembler

    def assemble(self, cached: Optional[CachedEntity]) -> Generator[Entity, None, None]:
        if cached is None:
            return
        entity = self.db.assemble(cached, sources=self.dataset.sources)
        if entity is not None:
            # This is already canonicalised thanks to `Database.load()`.
            if not self.db.cached:
                entity = self.db.resolver.apply(entity)
            if self.assembler is not None:
                entity = self.assembler(entity)
            yield entity

    def get_entity(self, id: str) -> Optional[Entity]:
        for cached in self.db.query(self.dataset, entity_id=id):
            for entity in self.assemble(cached):
                return entity
        return None

    def _get_inverted(self, id: str) -> Generator[Entity, None, None]:
        for cached in self.db.query(self.dataset, inverted_id=id):
            for entity in self.assemble(cached):
                yield entity

    def get_inverted(self, id: str) -> Generator[Tuple[Property, Entity], None, None]:
        for entity in self._get_inverted(id):
            for prop, value in entity.itervalues():
                if (
                    prop.type == registry.entity
                    and value == id
                    and prop.reverse is not None
                ):
                    yield prop.reverse, entity

    def _iter_entities(self) -> Generator[CachedEntity, None, None]:
        yield from self.db.query(self.dataset)

    def __iter__(self) -> Iterator[Entity]:
        for cached in self._iter_entities():
            for entity in self.assemble(cached):
                yield entity

    def __len__(self) -> int:
        return Statement.all_ids(self.dataset).count()

    def __repr__(self):
        return f"<DatasetLoader({self.dataset!r})>"


class CachedDatasetLoader(DatasetLoader):
    def get_entity(self, id: str) -> Optional[Entity]:
        cached = self.db.entities.get(id)
        for entity in self.assemble(cached):
            return entity
        return None

    def _get_inverted(self, id: str) -> Generator[Entity, None, None]:
        inverted = self.db.inverted.get(id)
        if inverted is None:
            return
        for entity_id in inverted:
            cached = self.db.entities.get(entity_id)
            yield from self.assemble(cached)

    def _iter_entities(self) -> Generator[CachedEntity, None, None]:
        yield from self.db.entities.values()

    def __repr__(self):
        return f"<CachedDatasetLoader({self.dataset!r})>"
