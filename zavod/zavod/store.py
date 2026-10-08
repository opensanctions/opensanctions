from followthemoney.exc import InvalidData
from followthemoney import Statement
from nomenklatura import duck
from nomenklatura.resolver import Linker
from nomenklatura.store.base import View as BaseView
from nomenklatura.store.duckdb_batch import DuckDBBatchStore

from zavod.logs import get_logger
from zavod.entity import Entity
from zavod.meta import Dataset
from zavod.archive import dataset_state_path
from zavod.runtime.lake import manifest_statements_view
from zavod.runtime.manifest import Manifest

log = get_logger(__name__)
View = BaseView[Dataset, Entity]


def get_store(manifest: Manifest, linker: Linker[Entity]) -> "LakeStore":
    return LakeStore(manifest, linker)


class LakeStore(DuckDBBatchStore[Dataset, Entity]):
    """Serve a manifest's pinned dataset versions out of their statement
    artifacts, via DuckDB.

    The store's relation is a plain view over the parquet (or pack) artifacts,
    resolved when the store is created, so the data is scanned exactly once -
    when a `view()` bakes its scoped, canonicalized statement table from it."""

    def __init__(self, manifest: Manifest, linker: Linker[Entity]) -> None:
        # The store holds only per-view materializations of immutable
        # artifacts, so every instance starts from a fresh database file.
        path = dataset_state_path(manifest.scope.name) / "store.duckdb"
        path.unlink(missing_ok=True)
        path.with_name(path.name + ".wal").unlink(missing_ok=True)
        conn = duck.connect(path)
        relation = manifest_statements_view(conn, manifest)
        super().__init__(manifest.scope, linker, conn, relation)
        self.manifest = manifest
        self.entity_class = Entity

    def assemble(self, statements: list[Statement]) -> Entity | None:
        try:
            entity = super().assemble(statements)
        except InvalidData as inv:
            dbg_stmts = [
                [s.dataset, s.entity_id, s.schema, s.prop, s.value] for s in statements
            ]
            log.error(f"Assemble error: {inv}", statements=dbg_stmts)
            return None
        return entity

    def close(self) -> None:
        super().close()
        self.conn.close()
