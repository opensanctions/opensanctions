from typing import Callable, Dict, Generator, List, Optional, Set, Tuple
from followthemoney import model
from followthemoney.types import registry
from followthemoney.property import Property
from zavod.logs import get_logger
from nomenklatura import Loader, Resolver

from opensanctions.core.dataset import Dataset
from opensanctions.core.entity import Entity
from opensanctions.core.db import engine_read
from opensanctions.core.statements import BASE, Statement
from opensanctions.core.statements import all_statements, count_entities

log = get_logger(__name__)


class CachedType(object):
    """In-memory store for an ID statement. These statements are used to
    define entities, including their schema and provenance."""

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
        self.canonical_id = stmt["canonical_id"]
        self.dataset = Dataset.require(stmt["dataset"])
        self.schema = model.schemata[stmt["schema"]]
        self.entity_id = stmt["entity_id"]
        self.first_seen = stmt["first_seen"]
        self.last_seen = stmt["last_seen"]
        self.target = stmt["target"]


class CachedProp(object):
    """In-memory store for a property-related statement."""

    __slots__ = ("value", "prop", "dataset")

    def __init__(self, stmt: Statement):
        self.dataset = Dataset.require(stmt["dataset"])
        schema = model.schemata[stmt["schema"]]
        try:
            self.prop: Optional[Property] = schema.properties[stmt["prop"]]
        except KeyError:
            self.prop = None
        self.value = stmt["value"]


CachedEntity = Tuple[Tuple[CachedType, ...], Tuple[CachedProp, ...]]
Assembler = Optional[Callable[[Entity], Entity]]


class Database(object):
    """A cache for entities from the database. This attempts to solve the issue of loading
    entities in the context of multiple scopes that occurs when exporting the data or
    when using the API. In those cases, it's useful to maintain one in-memory cache of all
    entities and then be able to assemble them using data from only some sources on demand.
    """

    def __init__(
        self,
        scope: Dataset,
        resolver: Resolver[Entity],
        cached: bool = False,
        external: bool = False,
    ):
        self.scope = scope
        self.cached = cached
        self.external = external
        self.resolver = resolver
        self.entities: Dict[str, CachedEntity] = {}
        self.inverted: Dict[str, Set[str]] = {}

    def view(self, dataset: Dataset, assembler: Assembler = None) -> "DatasetLoader":
        if self.cached:
            if not len(self.entities):
                self.load()
            return CachedDatasetLoader(self, dataset, assembler)
        return DatasetLoader(self, dataset, assembler)

    def load(self) -> None:
        """Pre-load all entity cache objects from the given scope dataset."""
        if not self.cached:
            return
        log.info("Loading database cache...", scope=self.scope)
        for cached in self.query(self.scope):
            canonical_id = cached[0][0].canonical_id
            self.entities[canonical_id] = cached
            for stmt in cached[1]:
                if stmt.prop is None or stmt.prop.type != registry.entity:
                    continue
                value_id = self.resolver.get_canonical(stmt.value)
                if value_id not in self.inverted:
                    self.inverted[value_id] = set()
                self.inverted[value_id].add(canonical_id)

    def query(
        self, dataset: Dataset, entity_id=None, inverted_id=None
    ) -> Generator[CachedEntity, None, None]:
        """Query the statement table for the given dataset and entity ID and return
        an entity cache object with the given properties."""
        canonical_id = None
        if entity_id is not None:
            canonical_id = self.resolver.get_canonical(entity_id)
        inverted_ids = None
        if inverted_id is not None:
            inverted_ids = self.resolver.get_referents(inverted_id)
        current_id = None
        types: List[CachedType] = []
        props: List[CachedProp] = []
        with engine_read() as conn:
            stmts = all_statements(
                conn,
                dataset=dataset,
                canonical_id=canonical_id,
                inverted_ids=inverted_ids,
                external=self.external,
            )
            for stmt in stmts:
                if stmt["canonical_id"] != current_id:
                    if len(types):
                        yield (tuple(types), tuple(props))
                    types = []
                    props = []
                current_id = stmt["canonical_id"]
                if stmt["prop"] == BASE:
                    types.append(CachedType(stmt))
                else:
                    props.append(CachedProp(stmt))
            if len(types):
                yield (tuple(types), tuple(props))

    def assemble(self, cached: CachedEntity, sources=Optional[Set[Dataset]]):
        """Build an entity proxy from a set of cached statements, considering
        only those statements that belong to the given sources."""
        entity = None
        for stmt in cached[0]:
            if sources is not None and stmt.dataset not in sources:
                continue
            if entity is None:
                data = {
                    "schema": stmt.schema,
                    "id": stmt.canonical_id,
                    "target": stmt.target,
                    "first_seen": stmt.first_seen,
                    "last_seen": stmt.last_seen,
                }
                entity = Entity(model, data)
            else:
                entity.add_schema(stmt.schema)
                entity.first_seen = min(entity.first_seen, stmt.first_seen)
                entity.last_seen = max(entity.last_seen, stmt.last_seen)
                entity.target = max(entity.target, stmt.target)
            entity.datasets.add(stmt.dataset.name)

        if entity is None:
            return None

        for prop in cached[1]:
            if sources is not None and prop.dataset not in sources:
                continue
            if prop.prop is None:
                continue
            entity.unsafe_add(prop.prop, prop.value, cleaned=True)

        entity.referents.update(self.resolver.get_referents(entity.id))
        return entity


class DatasetLoader(Loader[Dataset, Entity]):
    """This is a normal entity loader as specified in nomenklatura which uses the
    OpenSanctions database as a backend."""

    def __init__(self, database: Database, dataset: Dataset, assembler: Assembler):
        self.db = database
        self.dataset = dataset
        self.assembler = assembler

    def assemble(self, cached: Optional[CachedEntity]) -> Generator[Entity, None, None]:
        if cached is None:
            return
        entity = self.db.assemble(cached, sources=self.dataset.datasets)
        if entity is not None:
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
                if value == id and prop.reverse is not None:
                    yield prop.reverse, entity

    def _iter_entities(self) -> Generator[CachedEntity, None, None]:
        for cached in self.db.query(self.dataset):
            yield cached

    def __iter__(self) -> Generator[Entity, None, None]:
        for cached in self._iter_entities():
            for entity in self.assemble(cached):
                yield entity

    def __ken__(self) -> int:
        with engine_read() as conn:
            return count_entities(conn, self.dataset)

    def __repr__(self):
        return f"<DatasetLoader({self.dataset!r})>"


class CachedDatasetLoader(DatasetLoader):
    """Funky: this loader uses the cache from the `Database` object and tries to assemble
    a partial view of the entity as needed."""

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
            for entity in self.assemble(cached):
                yield entity

    def _iter_entities(self) -> Generator[CachedEntity, None, None]:
        for cached in self.db.entities.values():
            yield cached

    def __repr__(self):
        return f"<CachedDatasetLoader({self.dataset!r})>"
