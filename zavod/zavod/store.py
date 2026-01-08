import shutil
import plyvel  # type: ignore
from typing import List, Optional
from followthemoney.exc import InvalidData
from followthemoney import Statement
from nomenklatura.resolver import Linker
from nomenklatura.store.level import LevelDBStore, LevelDBView

from zavod.logs import get_logger
from zavod.entity import Entity
from zavod.meta import Dataset
from zavod.archive import dataset_state_path
from zavod.archive import iter_dataset_statements

log = get_logger(__name__)
View = LevelDBView[Dataset, Entity]


def get_store(dataset: Dataset, linker: Linker[Entity]) -> "Store":
    store = Store(dataset, linker)
    return store


class Store(LevelDBStore[Dataset, Entity]):
    def __init__(
        self,
        dataset: Dataset,
        linker: Linker[Entity],
    ):
        path = dataset_state_path(dataset.name) / "store"
        super().__init__(dataset, linker, path)
        self.entity_class = Entity

    def view(self, scope: Dataset, external: bool = False) -> View:
        return LevelDBView(self, scope, external=external)

    def assemble(self, statements: List[Statement]) -> Optional[Entity]:
        """Build an entity proxy from a set of cached statements, considering
        only those statements that belong to the given sources."""
        try:
            entity = super().assemble(statements)
        except InvalidData as inv:
            dbg_stmts = [
                [s.dataset, s.entity_id, s.schema, s.prop, s.value] for s in statements
            ]
            log.error("Assemble error: %s" % inv, statements=dbg_stmts)
            return None
        return entity

    def sync(self, clear: bool = False) -> None:
        if clear:
            self.clear()
        ds_key = f"dataset:{self.dataset.name}".encode("utf-8")
        if self.db.get(ds_key):
            return
        log.info("Building local LevelDB aggregator...", scope=self.dataset.name)
        idx = 0
        with self.writer() as writer:
            stmts = iter_dataset_statements(self.dataset, external=True)
            for idx, stmt in enumerate(stmts):
                if idx > 0 and idx % 50_000 == 0:
                    log.info(
                        "Indexing aggregator...",
                        statements=idx,
                        scope=self.dataset.name,
                        leaf=stmt.dataset,
                    )
                writer.add_statement(stmt)
        self.db.put(ds_key, b"1")
        self.optimize()
        log.info(
            "Local LevelDB aggregator is ready.",
            scope=self.dataset.name,
            statements=idx,
        )

    def clear(self) -> None:
        """Delete the working directory data for the latest version of the dataset
        from this store."""
        self.db.close()
        shutil.rmtree(self.path, ignore_errors=True)
        self.db = plyvel.DB(self.path.as_posix(), create_if_missing=True)
