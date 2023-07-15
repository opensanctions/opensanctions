from pathlib import Path
from typing import List, Optional
from zavod.logs import get_logger
from followthemoney.exc import InvalidData
from nomenklatura.statement import Statement
from nomenklatura.resolver import Resolver
from nomenklatura.store.level import LevelDBStore, LevelDBView
from nomenklatura.publish.dates import simplify_dates

from opensanctions import settings
from opensanctions.core.entity import Entity
from opensanctions.core.dataset import Dataset
from opensanctions.core.db import engine_read
from opensanctions.core.resolver import get_resolver
from opensanctions.core.statements import all_statements


log = get_logger(__name__)
View = LevelDBView[Dataset, Entity]


def get_store(dataset: Dataset, external: bool = False) -> "Store":
    resolver = get_resolver()
    aggregator_path = settings.DATA_PATH / "aggregator"
    aggregator_path.mkdir(parents=True, exist_ok=True)
    suffix = "internal" if external is False else "external"
    matching = [dataset]
    # matching.extend(dataset.parents)
    for ds in matching:
        dataset_path = aggregator_path / f"{ds.name}.{suffix}.db"
        if dataset_path.is_dir():
            return Store(dataset, resolver, dataset_path)
        if external is False:
            dataset_path = aggregator_path / f"{ds.name}.external.db"
            if dataset_path.is_dir():
                return Store(dataset, resolver, dataset_path)
    new_path = aggregator_path / f"{dataset.name}.{suffix}.db"
    store = Store(dataset, resolver, new_path)
    store.build(external=external)
    return store


def get_view(dataset: Dataset, external: bool = False) -> View:
    store = get_store(dataset, external=external)
    return store.default_view(external=external)


class Store(LevelDBStore[Dataset, Entity]):
    def __init__(
        self,
        dataset: Dataset,
        resolver: Resolver[Entity],
        path: Path,
    ):
        super().__init__(dataset, resolver, path)
        self.entity_class = Entity

    def build(self, external: bool = False):
        """Pre-load all entity cache objects from the given scope dataset."""
        log.info("Building local LevelDB aggregator...", scope=self.dataset.name)
        idx = 0
        with engine_read() as conn:
            with self.writer() as writer:
                stmts = all_statements(conn, self.dataset, external=external)
                for idx, stmt in enumerate(stmts):
                    if idx > 0 and idx % 100000 == 0:
                        log.info(
                            "Indexing aggregator...",
                            statements=idx,
                            scope=self.dataset.name,
                        )
                    writer.add_statement(stmt)
        log.info("Local cache complete.", scope=self.dataset.name, statements=idx)

    def assemble(self, statements: List[Statement]) -> Optional[Entity]:
        """Build an entity proxy from a set of cached statements, considering
        only those statements that belong to the given sources."""
        try:
            entity = super().assemble(statements)
        except InvalidData as inv:
            log.error("Assemble error: %s" % inv)
            return None
        if entity is not None:
            entity.extra_referents.update(self.resolver.get_referents(entity.id))
            entity = simplify_dates(entity)
            for stmt in statements:
                # The last_change attribute describes the latest checksum change
                # of any emitted component of the entity, which is stored in the BASE
                # field.
                if stmt.prop == Statement.BASE and stmt.first_seen is not None:
                    if entity.last_change is None:
                        entity.last_change = stmt.first_seen
                    else:
                        entity.last_change = max(entity.last_change, stmt.first_seen)
        return entity
