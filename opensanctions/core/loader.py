from typing import Callable, Dict, Generator, Iterable, List, Optional, Set, Tuple
from followthemoney.types import registry
from followthemoney.property import Property
from followthemoney.exc import InvalidData
from followthemoney import model
from zavod.logs import get_logger
from nomenklatura import Loader, Resolver
from nomenklatura.statement import Statement

from opensanctions.core.dataset import Dataset
from opensanctions.core.entity import Entity
from opensanctions.core.db import engine_read
from opensanctions.core.statements import all_statements, count_entities

log = get_logger(__name__)

Assembler = Optional[Callable[[Entity], Entity]]
Statements = Tuple[Statement, ...]


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
        self.entities: Dict[str, Statements] = {}
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
        for stmts in self.query(self.scope):
            canonical_id = stmts[0].canonical_id
            self.entities[canonical_id] = stmts
            for stmt in stmts:
                if stmt.prop is None or stmt.prop_type != registry.entity.name:
                    continue
                value_id = self.resolver.get_canonical(stmt.value)
                if value_id not in self.inverted:
                    self.inverted[value_id] = set()
                self.inverted[value_id].add(canonical_id)

    def query(
        self, dataset: Dataset, entity_id=None, inverted_id=None
    ) -> Generator[Statements, None, None]:
        """Query the statement table for the given dataset and entity ID and return
        an entity cache object with the given properties."""
        canonical_id = None
        if entity_id is not None:
            canonical_id = self.resolver.get_canonical(entity_id)
        inverted_ids = None
        if inverted_id is not None:
            inverted_ids = self.resolver.get_referents(inverted_id)
        current_id: Optional[str] = None
        entity: List[Statement] = []
        with engine_read() as conn:
            for stmt in all_statements(
                conn,
                dataset=dataset,
                canonical_id=canonical_id,
                inverted_ids=inverted_ids,
                external=self.external,
            ):
                if stmt.canonical_id != current_id:
                    if len(entity):
                        yield tuple(entity)
                    entity = []
                current_id = stmt.canonical_id
                entity.append(stmt)
            if len(entity):
                yield tuple(entity)

    def assemble(self, statements: Iterable[Statement], sources=Set[str]):
        """Build an entity proxy from a set of cached statements, considering
        only those statements that belong to the given sources."""
        entity: Optional[Entity] = None
        try:
            for stmt in statements:
                if stmt.dataset not in sources:
                    continue
                if entity is None:
                    data = {"schema": stmt.schema, "id": stmt.canonical_id}
                    entity = Entity(model, data, default_dataset=stmt.dataset)
                entity.add_statement(stmt)
        except InvalidData as inv:
            log.error("Assemble error: %s" % inv)
            return None
        if entity is not None and entity.id is not None:
            entity.referents.update(self.resolver.get_referents(entity.id))
        return entity


class DatasetLoader(Loader[Dataset, Entity]):
    """This is a normal entity loader as specified in nomenklatura which uses the
    OpenSanctions database as a backend."""

    def __init__(self, database: Database, dataset: Dataset, assembler: Assembler):
        self.db = database
        self.dataset = dataset
        self.scopes = set(self.dataset.scope_names)
        self.assembler = assembler

    def assemble(
        self, statements: Optional[Statements]
    ) -> Generator[Entity, None, None]:
        if statements is None:
            return
        entity = self.db.assemble(statements, sources=self.scopes)
        if entity is not None:
            entity = self.db.resolver.apply_properties(entity)
            if self.assembler is not None:
                entity = self.assembler(entity)
            yield entity

    def get_entity(self, id: str) -> Optional[Entity]:
        for statements in self.db.query(self.dataset, entity_id=id):
            for entity in self.assemble(statements):
                return entity
        return None

    def _get_inverted(self, id: str) -> Generator[Entity, None, None]:
        for statements in self.db.query(self.dataset, inverted_id=id):
            for entity in self.assemble(statements):
                yield entity

    def get_inverted(self, id: str) -> Generator[Tuple[Property, Entity], None, None]:
        for entity in self._get_inverted(id):
            for prop, value in entity.itervalues():
                if value == id and prop.reverse is not None:
                    yield prop.reverse, entity

    def _iter_entities(self) -> Generator[Statements, None, None]:
        for statements in self.db.query(self.dataset):
            yield statements

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
        statements = self.db.entities.get(id)
        for entity in self.assemble(statements):
            return entity
        return None

    def _get_inverted(self, id: str) -> Generator[Entity, None, None]:
        inverted = self.db.inverted.get(id)
        if inverted is None:
            return
        for entity_id in inverted:
            statements = self.db.entities.get(entity_id)
            for entity in self.assemble(statements):
                yield entity

    def _iter_entities(self) -> Generator[Statements, None, None]:
        for statements in self.db.entities.values():
            yield statements

    def __repr__(self):
        return f"<CachedDatasetLoader({self.dataset!r})>"
