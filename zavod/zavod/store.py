import time
from typing import List, Optional
from datetime import datetime, timedelta
from redis.exceptions import ConnectionError, TimeoutError
from followthemoney.exc import InvalidData
from nomenklatura.statement import Statement
from nomenklatura.resolver import Linker
from nomenklatura.versions import Version
from nomenklatura.store.versioned import VersionedRedisStore, VersionedRedisView
from nomenklatura.publish.dates import simplify_dates
from nomenklatura.publish.edges import simplify_undirected

from zavod.logs import get_logger
from zavod.entity import Entity
from zavod.meta import Dataset
from zavod.dedupe import get_dataset_resolver, get_dataset_linker
from zavod.archive import iter_previous_statements, iter_dataset_versions

log = get_logger(__name__)
View = VersionedRedisView[Dataset, Entity]

RETAIN_DAYS = 4
RETAIN_TIME = datetime.now().replace(tzinfo=None) - timedelta(days=RETAIN_DAYS)


def get_store(
    dataset: Dataset, external: bool = False, linker: bool = False
) -> "Store":
    if linker:
        linker_inst = get_dataset_linker(dataset)
    else:
        linker_inst = get_dataset_resolver(dataset)
    # aggregator_path = dataset_state_path(dataset.name)
    # suffix = "external" if external else "internal"
    # dataset_path = aggregator_path / f"{dataset.name}.{suffix}.store"
    # if dataset_path.is_dir():
    #     return Store(dataset, linker_inst, dataset_path)
    # if not external:
    #     external_path = aggregator_path / f"{dataset.name}.external.store"
    #     if external_path.is_dir():
    #         return get_store(dataset, external=True, linker=linker)
    store = Store(dataset, linker_inst)
    sync_scope(store, dataset)
    # store.build(external=external)
    return store


def clear_store(dataset: Dataset) -> None:
    """Delete the store graph for the given dataset."""
    store = get_store(dataset)
    versions = store.get_history(dataset.name)
    for version in versions:
        store.drop_version(dataset.name, version)
    # aggregator_path = dataset_state_path(dataset.name)
    # external_path = aggregator_path / f"{dataset.name}.external.store"
    # if external_path.exists():
    #     shutil.rmtree(external_path, ignore_errors=True)
    # internal_path = aggregator_path / f"{dataset.name}.internal.store"
    # if internal_path.exists():
    #     shutil.rmtree(internal_path, ignore_errors=True)


def get_view(dataset: Dataset, external: bool = False, linker: bool = False) -> View:
    store = get_store(dataset, external=external, linker=linker)
    return store.view(dataset, external=external)


def sync_dataset(store: VersionedRedisStore, dataset: Dataset):
    versions = store.get_history(dataset.name)
    for version in iter_dataset_versions(dataset.name):
        if version.id in versions:
            return
        stmts = 0
        writer = store.writer(dataset, version=version.id)
        for stmt in iter_previous_statements(dataset, version=version.id):
            stmts += 1
            if stmts % 10_000 == 0:
                log.info(
                    "Loading [%s]: %d..." % (dataset.name, stmts),
                    version=version.id,
                )
            writer.add_statement(stmt)
        if stmts > 0:
            writer.flush()
            writer.release()
            for old in versions:
                if old != version.id:
                    old_version = Version.from_string(old)
                    if old_version.dt < RETAIN_TIME:
                        log.info("Drop old version: %s" % dataset.name, version=old)
                        store.drop_version(dataset.name, old)
            return


def sync_scope(store: VersionedRedisStore, scope: Dataset) -> None:
    for dataset in scope.leaves:
        try:
            sync_dataset(store, dataset)
        except (ConnectionError, TimeoutError):
            log.exception("Connection error while loading dataset: %s" % dataset.name)
            time.sleep(10)


class Store(VersionedRedisStore[Dataset, Entity]):
    def __init__(
        self,
        dataset: Dataset,
        linker: Linker[Entity],
    ):
        super().__init__(dataset, linker)
        self.entity_class = Entity

    # def build(self, external: bool = False) -> None:
    #     """Pre-load all entity cache objects from the given scope dataset."""
    #     log.info("Building local LevelDB aggregator...", scope=self.dataset.name)
    #     idx = 0
    #     with self.writer() as writer:
    #         stmts = iter_dataset_statements(self.dataset, external=external)
    #         for idx, stmt in enumerate(stmts):
    #             if idx > 0 and idx % 100000 == 0:
    #                 log.info(
    #                     "Indexing aggregator...",
    #                     statements=idx,
    #                     scope=self.dataset.name,
    #                 )
    #             writer.add_statement(stmt)
    #     log.info("Local cache complete.", scope=self.dataset.name, statements=idx)

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
                entity.extra_referents.update(self.linker.get_referents(entity.id))
            entity = simplify_dates(entity)
            entity = simplify_undirected(entity)
        return entity
