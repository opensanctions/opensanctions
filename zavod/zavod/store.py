import shutil
from pathlib import Path
from typing import List, Optional
from followthemoney.exc import InvalidData
from nomenklatura.statement import Statement
from nomenklatura.resolver import Resolver
from nomenklatura.store.base import View as BaseView
from nomenklatura.store.level import LevelDBStore
from nomenklatura.publish.dates import simplify_dates
from nomenklatura.publish.edges import simplify_undirected

from zavod.logs import get_logger
from zavod.entity import Entity
from zavod.meta import Dataset
from zavod.dedupe import get_dataset_resolver
from zavod.archive import dataset_state_path, iter_dataset_statements

log = get_logger(__name__)
View = BaseView[Dataset, Entity]


def get_store(dataset: Dataset, external: bool = False) -> "Store":
    resolver = get_dataset_resolver(dataset)
    aggregator_path = dataset_state_path(dataset.name)
    suffix = "external" if external else "internal"
    dataset_path = aggregator_path / f"{dataset.name}.{suffix}.store"
    if dataset_path.is_dir():
        return Store(dataset, resolver, dataset_path)
    if not external:
        external_path = aggregator_path / f"{dataset.name}.external.store"
        if external_path.is_dir():
            return get_store(dataset, external=True)
    store = Store(dataset, resolver, dataset_path)
    store.build(external=external)
    return store


def clear_store(dataset: Dataset) -> None:
    """Delete the store graph for the given dataset."""
    aggregator_path = dataset_state_path(dataset.name)
    external_path = aggregator_path / f"{dataset.name}.external.store"
    if external_path.exists():
        shutil.rmtree(external_path, ignore_errors=True)
    internal_path = aggregator_path / f"{dataset.name}.internal.store"
    if internal_path.exists():
        shutil.rmtree(internal_path, ignore_errors=True)


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

    def build(self, external: bool = False) -> None:
        """Pre-load all entity cache objects from the given scope dataset."""
        log.info("Building local LevelDB aggregator...", scope=self.dataset.name)
        idx = 0
        with self.writer() as writer:
            stmts = iter_dataset_statements(self.dataset, external=external)
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
            if entity.id is not None:
                entity.extra_referents.update(self.resolver.get_referents(entity.id))
            entity = simplify_dates(entity)
            entity = simplify_undirected(entity)
        return entity
