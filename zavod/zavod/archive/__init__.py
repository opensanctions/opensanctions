import csv
import shutil
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Optional, Generator, TextIO, Set
from rigour.mime.types import JSON
from nomenklatura.statement import Statement
from nomenklatura.statement.serialize import unpack_row
from nomenklatura.versions import Version, VersionHistory

from zavod import settings
from zavod.logs import get_logger
from zavod.archive.backend import get_archive_backend, ArchiveObject

if TYPE_CHECKING:
    from zavod.meta.dataset import Dataset

log = get_logger(__name__)
StatementGen = Generator[Statement, None, None]
DATASETS = "datasets"
ARTIFACTS = "artifacts"
STATEMENTS_FILE = "statements.pack"
DELTA_FILE = "entities.delta.json"
STATISTICS_FILE = "statistics.json"
ISSUES_LOG = "issues.log"
ISSUES_FILE = "issues.json"
RESOURCES_FILE = "resources.json"
INDEX_FILE = "index.json"
CATALOG_FILE = "catalog.json"
VERSIONS_FILE = "versions.json"


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
        obj = get_artifact_object(dataset_name, resource, version)
        if obj is not None:
            log.info(
                "Backfilling dataset artifact...",
                dataset=dataset_name,
                resource=resource,
                object=obj.name,
            )
            obj.backfill(path)
    return path


def _get_history_object(
    dataset_name: str, version: Optional[str] = None
) -> Optional[str]:
    backend = get_archive_backend()
    name = f"{ARTIFACTS}/{dataset_name}/{VERSIONS_FILE}"
    if version is not None:
        name = f"{ARTIFACTS}/{dataset_name}/{version}/{VERSIONS_FILE}"
    obj = backend.get_object(name)
    if obj.exists():
        return obj.open().read()
    return None


def iter_dataset_versions(dataset_name: str) -> Generator[Version, None, None]:
    """Iterate over all versions of a given dataset."""
    data = _get_history_object(dataset_name)
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
        data = _get_history_object(dataset_name, history.items[0].id)


def get_artifact_object(
    dataset_name: str, resource: str, version: Optional[str] = None
) -> Optional[ArchiveObject]:
    backend = get_archive_backend()
    if version is not None:
        name = f"{ARTIFACTS}/{dataset_name}/{version}/{resource}"
        obj = backend.get_object(name)
        if obj.exists():
            return obj
    else:
        for v in iter_dataset_versions(dataset_name):
            name = f"{ARTIFACTS}/{dataset_name}/{v.id}/{resource}"
            obj = backend.get_object(name)
            if obj.exists():
                return obj

    # FIXME: legacy fallback option of using the latest release
    # REMOVE THIS AFTER MIGRATION
    name = f"{DATASETS}/latest/{dataset_name}/{resource}"
    obj = backend.get_object(name)
    if obj.exists():
        return obj
    return None


def publish_dataset_history(dataset_name: str, version: Version) -> None:
    """Publish the history of versions for a given dataset to the artifact directory."""
    data = _get_history_object(dataset_name)
    history = VersionHistory.from_json(data or "{}")
    if version not in history.items:
        history = history.append(version)
    backend = get_archive_backend()
    path = dataset_resource_path(dataset_name, VERSIONS_FILE)

    with open(path, "w", encoding="utf-8") as fh:
        fh.write(history.to_json())

    name = f"{ARTIFACTS}/{dataset_name}/{VERSIONS_FILE}"
    obj = backend.get_object(name)
    obj.publish(path, mime_type=JSON)
    name = f"{ARTIFACTS}/{dataset_name}/{version.id}/{VERSIONS_FILE}"
    obj = backend.get_object(name)
    obj.publish(path, mime_type=JSON)


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
    obj = backend.get_object(name)
    obj.publish(path, mime_type=mime_type)


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
    release_object.publish(path, mime_type=mime_type)

    if latest and settings.RELEASE != "latest":
        latest_name = f"{DATASETS}/latest/{resource}"
        latest_object = backend.get_object(latest_name)
        latest_object.republish(release_name)


def _read_fh_statements(fh: TextIO, external: bool) -> StatementGen:
    for cells in csv.reader(fh):
        stmt = unpack_row(cells, Statement)
        if not external and stmt.external:
            continue
        yield stmt


def iter_dataset_statements(dataset: "Dataset", external: bool = True) -> StatementGen:
    """Create a generator that yields all statements in the given dataset."""
    for scope in dataset.leaves:
        yield from _iter_scope_statements(scope, external=external)


def _iter_scope_statements(dataset: "Dataset", external: bool = True) -> StatementGen:
    path = dataset_resource_path(dataset.name, STATEMENTS_FILE)
    if path.exists():
        with open(path, "r", encoding="utf-8") as fh:
            yield from _read_fh_statements(fh, external)
        return

    obj = get_artifact_object(dataset.name, STATEMENTS_FILE)
    if obj is not None:
        log.info(
            "Streaming backfilled statements...",
            backfill_dataset=dataset.name,
            object=obj.name,
        )
        with obj.open() as fh:
            yield from _read_fh_statements(fh, external)
        return
    log.error(f"Cannot load statements for: {dataset.name}")


def iter_previous_statements(
    dataset: "Dataset", external: bool = True, version: Optional[str] = None
) -> StatementGen:
    """Load the statements from the previous release of the dataset by streaming them
    from the data archive."""
    for scope in dataset.leaves:
        obj = get_artifact_object(dataset.name, STATEMENTS_FILE, version)

        if obj is not None:
            log.info(
                "Streaming backfilled statements...",
                dataset=scope.name,
                object=obj.name,
            )
            with obj.open() as fh:
                yield from _read_fh_statements(fh, external)
