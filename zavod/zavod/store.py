import time
from typing import List, Optional, Iterable
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
from zavod.runtime.versions import get_latest
from zavod.dedupe import get_dataset_resolver, get_dataset_linker
from zavod.archive import iter_previous_statements, iter_dataset_versions
from zavod.archive import iter_dataset_statements

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
    store = Store(dataset, linker_inst)
    sync_scope(store, dataset)
    return store


def clear_store(dataset: Dataset) -> None:
    """Delete the store graph for the given dataset."""
    store = get_store(dataset)
    latest = get_latest(dataset.name, backfill=False)
    if latest is not None:
        store.drop_version(dataset.name, latest.id)
    # versions = store.get_history(dataset.name)
    # for version in versions:
    #     store.drop_version(dataset.name, version)


def get_view(dataset: Dataset, external: bool = False, linker: bool = False) -> View:
    store = get_store(dataset, external=external, linker=linker)
    return store.view(dataset, external=external)


def _write_statements(
    store: VersionedRedisStore,
    dataset: Dataset,
    version: str,
    statements: Iterable[Statement],
) -> int:
    stmts = 0
    writer = store.writer(dataset, version=version)
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
        writer.release()
    return stmts


def sync_dataset(store: VersionedRedisStore, dataset: Dataset):
    versions = store.get_history(dataset.name)
    latest = get_latest(dataset.name, backfill=False)
    if latest is not None and latest.id not in versions:
        statements = iter_dataset_statements(dataset)
        _write_statements(store, dataset, latest.id, statements)

    for version in iter_dataset_versions(dataset.name):
        if version.id in versions:
            return
        statements = iter_previous_statements(dataset, version=version.id)
        count = _write_statements(store, dataset, version.id, statements)
        if count > 0:
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
