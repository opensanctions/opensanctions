import time
from typing import List, Optional, Iterable, Dict
from datetime import timedelta
from redis.exceptions import ConnectionError, TimeoutError
from followthemoney.exc import InvalidData
from nomenklatura.statement import Statement
from nomenklatura.resolver import Linker
from nomenklatura.versions import Version
from nomenklatura.store.versioned import VersionedRedisStore, VersionedRedisView
from nomenklatura.publish.dates import simplify_dates
from nomenklatura.publish.edges import simplify_undirected

from zavod import settings
from zavod.logs import get_logger
from zavod.entity import Entity
from zavod.meta import Dataset
from zavod.runtime.versions import get_latest
from zavod.archive import iter_previous_statements, iter_dataset_versions
from zavod.archive import iter_local_statements

log = get_logger(__name__)
View = VersionedRedisView[Dataset, Entity]


def get_store(dataset: Dataset, linker: Linker[Entity]) -> "Store":
    store = Store(dataset, linker)
    return store


def _write_statements(
    store: "Store",
    dataset: Dataset,
    version: str,
    statements: Iterable[Statement],
) -> int:
    stmts = 0
    writer = store.writer(dataset, version=version, timestamps=False)
    for stmt in statements:
        stmts += 1
        if stmts % 10_000 == 0:
            log.info(
                "Loading [%s]: %d..." % (dataset.name, stmts),
                version=version,
            )
        writer.add_statement(stmt)
    if stmts > 0:
        writer.flush()
    return stmts


def sync_dataset(store: "Store", dataset: Dataset) -> None:
    versions = store.get_history(dataset.name)
    latest = get_latest(dataset.name, backfill=False)
    if latest is not None and not store.has_version(dataset.name, latest.id):
        try:
            statements = iter_local_statements(dataset)
            _write_statements(store, dataset, latest.id, statements)
        except FileNotFoundError:
            log.info("No local statements found", dataset=dataset.name)

    retain_delta = timedelta(days=settings.STORE_RETAIN_DAYS)
    retain_time = settings.RUN_TIME - retain_delta
    for version in iter_dataset_versions(dataset.name):
        if version.id in versions:
            return
        statements = iter_previous_statements(dataset, version=version.id)
        count = _write_statements(store, dataset, version.id, statements)
        if count > 0:
            store.release_version(dataset.name, version.id)
            for old in versions:
                if old != version.id:
                    old_version = Version.from_string(old)
                    if old_version.dt < retain_time:
                        log.info("Drop old version: %s" % dataset.name, version=old)
                        store.drop_version(dataset.name, old)
            return


class Store(VersionedRedisStore[Dataset, Entity]):
    def __init__(
        self,
        dataset: Dataset,
        linker: Linker[Entity],
    ):
        super().__init__(dataset, linker)
        self.entity_class = Entity

    def view(
        self, dataset: Dataset, external: bool = False, versions: Dict[str, str] = {}
    ) -> View:
        """Define source dataset versions for the store view."""
        versions_: Dict[str, str] = {}
        for ds in dataset.leaf_names:
            if ds in versions:
                versions_[ds] = versions[ds]
                continue
            version = get_latest(ds, backfill=False)
            if version is not None:
                versions_[ds] = version.id
        return super().view(dataset, external=external, versions=versions_)

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

    def sync(self, clear: bool = False) -> None:
        if clear:
            self.clear_latest()
        for dataset in self.dataset.leaves:
            try:
                sync_dataset(self, dataset)
            except (ConnectionError, TimeoutError):
                log.exception(
                    "Connection error while loading dataset: %s" % dataset.name
                )
                time.sleep(10)

    def clear_latest(self) -> None:
        """Delete the working directory data for the latest version of the dataset
        from this store."""
        for ds in self.dataset.leaves:
            latest = get_latest(ds.name, backfill=False)
            if latest is not None:
                self.drop_version(ds.name, latest.id)

    # def clear_all(self) -> None:
    #     """Delete all data for the dataset from this store."""
    #     for ds in self.dataset.leaves:
    #         for version in self.get_history(ds.name):
    #             self.drop_version(ds.name, version)
    #     self.clear_latest()
