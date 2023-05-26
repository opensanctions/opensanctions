import orjson
import plyvel
from typing import Callable, Generator, List, Iterable, Optional, Set, Tuple
from followthemoney.types import registry
from followthemoney.property import Property
from followthemoney.exc import InvalidData
from followthemoney import model
from zavod.logs import get_logger
from sqlalchemy.future import select
from nomenklatura import Loader, Resolver
from nomenklatura.statement import Statement
from nomenklatura.util import iso_datetime

from opensanctions import settings
from opensanctions.core.dataset import Dataset
from opensanctions.core.entity import Entity
from opensanctions.core.db import engine_read, stmt_table

log = get_logger(__name__)

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
        self.path = settings.DATA_PATH / f"{scope.name}.lvldb"
        self.lvl = plyvel.DB(self.path.as_posix(), create_if_missing=True)
        self.scope = scope
        self.external = external
        self.resolver = resolver

    def view(self, dataset: Dataset, assembler: Assembler = None) -> "DatasetLoader":
        self.build()
        return DatasetLoader(self, dataset, assembler)

    def iter_statements(self) -> Generator[Statement, None, None]:
        with engine_read() as conn:
            q = select(stmt_table)
            if self.scope.name != Dataset.ALL:
                q = q.filter(stmt_table.c.dataset.in_(self.scope.scope_names))

            # q = q.order_by(stmt_table.c.canonical_id.asc())
            conn = conn.execution_options(stream_results=True)
            cursor = conn.execute(q)
            while True:
                rows = cursor.fetchmany(50000)
                if not rows:
                    break
                for row in rows:
                    yield Statement.from_db_row(row)

    def build(self) -> None:
        """Pre-load all entity cache objects from the given scope dataset."""
        if self.lvl.get(b"x.done") is not None:
            return
        log.info("Building local LevelDB cache...", scope=self.scope.name)
        wb = self.lvl.write_batch()
        for idx, stmt in enumerate(self.iter_statements()):
            if idx > 0 and idx % 100000 == 0:
                log.info(
                    "Indexing local cache...",
                    statements=idx,
                    scope=self.scope.name,
                )
                wb.write()
                wb = self.lvl.write_batch()
            stmt.canonical_id = self.resolver.get_canonical(stmt.entity_id)
            data = orjson.dumps(stmt.to_dict())
            key = f"e:{stmt.canonical_id}:{stmt.dataset}".encode("utf-8")
            wb.put(key, stmt.schema.encode("utf-8"))
            key = f"s:{stmt.canonical_id}:{stmt.id}".encode("utf-8")
            wb.put(key, data)
            if stmt.prop_type == registry.entity.name:
                vc = self.resolver.get_canonical(stmt.value)
                key = f"i:{vc}:{stmt.canonical_id}".encode("utf-8")
                wb.put(key, stmt.canonical_id.encode("utf-8"))
        wb.put(b"x.done", b"yes")
        log.info("Local cache complete.", scope=self.scope.name, statements=idx)
        wb.write()

    def assemble(self, statements: Iterable[Statement]):
        """Build an entity proxy from a set of cached statements, considering
        only those statements that belong to the given sources."""
        entity: Optional[Entity] = None
        try:
            for stmt in statements:
                if entity is None:
                    data = {"schema": stmt.schema, "id": stmt.canonical_id}
                    entity = Entity(model, data, default_dataset=stmt.dataset)
                    entity.last_change = stmt.first_seen
                if stmt.prop == Statement.BASE:
                    entity.last_change = max(entity.last_change, stmt.first_seen)
                entity.add_statement(stmt)
        except InvalidData as inv:
            log.error("Assemble error: %s" % inv)
            return None
        if entity is not None and entity.id is not None:
            entity.referents.update(self.resolver.get_referents(entity.id))
        return entity


class DatasetLoader(Loader[Dataset, Entity]):
    """This is a normal entity loader as specified in nomenklatura which uses the
    local KV cache as a backend."""

    def __init__(self, database: Database, dataset: Dataset, assembler: Assembler):
        self.db = database
        self.dataset = dataset
        self.scopes = set(self.dataset.scope_names)
        self.assembler = assembler

    def assemble(self, statements: List[Statement]) -> Generator[Entity, None, None]:
        entity = self.db.assemble(statements)
        if entity is not None:
            entity = self.db.resolver.apply_properties(entity)
            if self.assembler is not None:
                entity = self.assembler(entity)
            yield entity

    def get_entity(self, id: str) -> Optional[Entity]:
        statements: List[Statement] = []
        prefix = f"s:{id}:".encode("utf-8")
        with self.db.lvl.iterator(prefix=prefix, include_key=False) as it:
            for v in it:
                data = orjson.loads(v)
                if not self.db.external and data.get("external"):
                    continue
                if data.get("dataset") in self.scopes:
                    data["first_seen"] = iso_datetime(data["first_seen"])
                    data["last_seen"] = iso_datetime(data["last_seen"])
                    statements.append(Statement.from_dict(data))
        if len(statements):
            for entity in self.assemble(statements):
                return entity
        return None

    def _get_inverted(self, id: str) -> Generator[Entity, None, None]:
        prefix = f"i:{id}:".encode("utf-8")
        with self.db.lvl.iterator(prefix=prefix, include_key=False) as it:
            for v in it:
                entity = self.get_entity(v.decode("utf-8"))
                if entity is not None:
                    yield entity

    def get_inverted(self, id: str) -> Generator[Tuple[Property, Entity], None, None]:
        for entity in self._get_inverted(id):
            for prop, value in entity.itervalues():
                if value == id and prop.reverse is not None:
                    yield prop.reverse, entity

    def __iter__(self) -> Generator[Entity, None, None]:
        prefix = f"e:".encode("utf-8")
        with self.db.lvl.iterator(prefix=prefix, include_value=False) as it:
            current_id: Optional[str] = None
            current_match = False
            for k in it:
                _, entity_id, dataset = k.decode("utf-8").split(":", 2)
                if entity_id != current_id:
                    current_id = entity_id
                    current_match = False
                if current_match:
                    continue
                if dataset in self.scopes:
                    current_match = True
                    entity = self.get_entity(entity_id)
                    if entity is not None:
                        yield entity

    def __len__(self) -> int:
        raise NotImplementedError()

    def __repr__(self):
        return f"<DatasetLoader({self.dataset!r})>"
