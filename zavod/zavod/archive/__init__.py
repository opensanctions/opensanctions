import shutil
from pathlib import Path
from functools import lru_cache
from typing import TYPE_CHECKING
from typing import Optional, Generator, TextIO, Set
from rigour.mime.types import JSON
from followthemoney import Statement
from followthemoney.statement.serialize import read_pack_statements_decoded
from nomenklatura.versions import Version, VersionHistory

from zavod import settings
from zavod.logs import get_logger
from zavod.archive.backend import get_archive_backend, ArchiveObject
from zavod.archive.cdn import invalidate_archive_cache

if TYPE_CHECKING:
    from zavod.meta.dataset import Dataset

log = get_logger(__name__)
StatementGen = Generator[Statement, None, None]
DATASETS = "datasets"
ARTIFACTS = "artifacts"
STATEMENTS_FILE = "statements.pack"
HASH_FILE = "entities.hash"
DELTA_EXPORT_FILE = "entities.delta.json"
DELTA_INDEX_FILE = "delta.json"
STATISTICS_FILE = "statistics.json"
ISSUES_LOG = "issues.log"
ISSUES_FILE = "issues.json"
RESOURCES_FILE = "resources.json"
INDEX_FILE = "index.json"
CATALOG_FILE = "catalog.json"
VERSIONS_FILE = "versions.json"
ARTIFACT_FILES = [
    ISSUES_FILE,
    ISSUES_LOG,
    INDEX_FILE,
    STATEMENTS_FILE,
    STATISTICS_FILE,
    VERSIONS_FILE,
    RESOURCES_FILE,
    DELTA_EXPORT_FILE,
    DELTA_INDEX_FILE,
    HASH_FILE,
]
TTL_SHORT = 10 * 60
TTL_MEDIUM = 24 * 60 * 60
TTL_LONG = 7 * 24 * 60 * 60


def datasets_path() -> Path:
    return settings.DATA_PATH / DATASETS


def dataset_data_path(dataset_name: str) -> Path:
    path = datasets_path() / dataset_name
    path.mkdir(parents=True, exist_ok=True)
    return path.resolve()


def dataset_state_path(dataset_name: str) -> Path:
    """The state directory is outside the main data directory and is used for temporary
    processing artifacts (like the materialised graph, and the timestamp index)."""
    path = dataset_data_path(dataset_name) / "_state"
    path.mkdir(parents=True, exist_ok=True)
    return path.resolve()


def clear_data_path(dataset_name: str) -> None:
    """Delete all recorded data for a given dataset."""
    shutil.rmtree(dataset_data_path(dataset_name), ignore_errors=True)


def dataset_resource_path(dataset_name: str, resource: str) -> Path:
    dataset_path = dataset_data_path(dataset_name)
    return dataset_path.joinpath(resource)


def get_dataset_artifact(
    dataset_name: str,
    resource: str,
    backfill: bool = True,
    version: Optional[str] = None,
) -> Path:
    path = dataset_resource_path(dataset_name, resource)
    if path.exists():
        return path
    if backfill:
        object = get_artifact_object(dataset_name, resource, version)
        if object is not None:
            log.info(
                "Backfilling dataset artifact...",
                current=dataset_name,
                resource=resource,
                object=object.name,
            )
            object.backfill(path)
    return path


@lru_cache(maxsize=1000)
def get_versions_data(
    dataset_name: str, version: Optional[str] = None
) -> Optional[str]:
    backend = get_archive_backend()
    name = f"{ARTIFACTS}/{dataset_name}/{VERSIONS_FILE}"
    if version is not None:
        name = f"{ARTIFACTS}/{dataset_name}/{version}/{VERSIONS_FILE}"
    object = backend.get_object(name)
    if object.exists():
        return object.open().read()
    return None


def iter_dataset_versions(dataset_name: str) -> Generator[Version, None, None]:
    """Iterate over all versions of a given dataset."""
    data = get_versions_data(dataset_name)
    seen: Set[str] = set()
    while True:
        if data is None:
            break
        history = VersionHistory.from_json(data)
        for version in history.items[::-1]:
            if version.id not in seen:
                yield version
                seen.add(version.id)
        if len(history.items) < 2:
            break
        data = get_versions_data(dataset_name, history.items[0].id)


def get_artifact_object(
    dataset_name: str, resource: str, version: Optional[str] = None
) -> Optional[ArchiveObject]:
    backend = get_archive_backend()
    if version is not None:
        name = f"{ARTIFACTS}/{dataset_name}/{version}/{resource}"
        object = backend.get_object(name)
        if object.exists():
            return object
    else:
        for v in iter_dataset_versions(dataset_name):
            name = f"{ARTIFACTS}/{dataset_name}/{v.id}/{resource}"
            object = backend.get_object(name)
            if object.exists():
                return object

    # FIXME: legacy fallback option of using the latest release
    # REMOVE THIS AFTER MIGRATION
    name = f"{DATASETS}/latest/{dataset_name}/{resource}"
    object = backend.get_object(name)
    if object.exists():
        return object
    return None


def publish_dataset_version(dataset_name: str) -> None:
    """Publish the history of versions for a given dataset to the artifact directory."""
    path = dataset_resource_path(dataset_name, VERSIONS_FILE)
    if not path.exists():
        raise RuntimeError(f"Version history not found: {dataset_name}")

    backend = get_archive_backend()
    name = f"{ARTIFACTS}/{dataset_name}/{VERSIONS_FILE}"
    object = backend.get_object(name)
    object.publish(path, mime_type=JSON, ttl=TTL_MEDIUM)
    invalidate_archive_cache(name)
    get_versions_data.cache_clear()


def publish_artifact(
    path: Path,
    dataset_name: str,
    version: Version,
    resource: str,
    mime_type: Optional[str] = None,
) -> None:
    """Publish a file in the given versions artifact directory of the dataset."""
    name = f"{ARTIFACTS}/{dataset_name}/{version.id}/{resource}"
    backend = get_archive_backend()
    object = backend.get_object(name)
    object.publish(path, mime_type=mime_type, ttl=TTL_LONG)


def publish_resource(
    path: Path,
    dataset_name: Optional[str],
    resource: str,
    latest: bool = True,
    mime_type: Optional[str] = None,
) -> None:
    """Resources are files published to the main publication directory of the dataset."""
    backend = get_archive_backend()
    if dataset_name is not None:
        assert path.relative_to(dataset_data_path(dataset_name))
        resource = f"{dataset_name}/{resource}"
    release_name = f"{DATASETS}/{settings.RELEASE}/{resource}"
    release_object = backend.get_object(release_name)
    release_object.publish(path, mime_type=mime_type, ttl=TTL_MEDIUM)
    invalidate_archive_cache(release_name)

    if latest and settings.RELEASE != "latest":
        latest_name = f"{DATASETS}/latest/{resource}"
        latest_object = backend.get_object(latest_name)
        latest_object.republish(release_name)
        invalidate_archive_cache(latest_name)


def _read_fh_statements(fh: TextIO, external: bool) -> StatementGen:
    for stmt in read_pack_statements_decoded(fh):
        if not external and stmt.external:
            continue
        yield stmt


def iter_dataset_statements(dataset: "Dataset", external: bool = True) -> StatementGen:
    """Create a generator that yields all statements in the given dataset."""
    for scope in dataset.leaves:
        yield from _iter_scope_statements(scope, external=external)


def iter_local_statements(dataset: "Dataset", external: bool = True) -> StatementGen:
    """Create a generator that yields all statements in the given dataset."""
    assert not dataset.is_collection
    path = dataset_resource_path(dataset.name, STATEMENTS_FILE)
    if settings.ARCHIVE_BACKFILL_STATEMENTS:
        get_dataset_artifact(dataset.name, STATEMENTS_FILE)
    if not path.exists():
        raise FileNotFoundError(f"Statements not found: {dataset.name}")
    with open(path, "r") as fh:
        yield from _read_fh_statements(fh, external)


def _iter_scope_statements(dataset: "Dataset", external: bool = True) -> StatementGen:
    try:
        yield from iter_local_statements(dataset, external=external)
        return
    except FileNotFoundError:
        pass

    object = get_artifact_object(dataset.name, STATEMENTS_FILE)
    if object is not None:
        log.info(
            "Streaming statements...",
            current=dataset.name,
            object=object.name,
        )
        with object.open() as fh:
            yield from _read_fh_statements(fh, external)
        return
    log.error(f"Cannot load statements for: {dataset.name}")


def iter_previous_statements(
    dataset: "Dataset", external: bool = True, version: Optional[str] = None
) -> StatementGen:
    """Load the statements from the previous release of the dataset by streaming them
    from the data archive."""
    for scope in dataset.leaves:
        object = get_artifact_object(dataset.name, STATEMENTS_FILE, version)
        if object is not None:
            log.info(
                "Streaming backfilled statements...",
                current=scope.name,
                object=object.name,
            )
            with object.open() as fh:
                yield from _read_fh_statements(fh, external)
