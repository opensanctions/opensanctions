import csv
import shutil
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Optional, Generator, TextIO
from functools import lru_cache
from rigour.mime.types import JSON

from zavod.logs import get_logger
from nomenklatura.statement import Statement
from nomenklatura.statement.serialize import unpack_row
from nomenklatura.versions import Version, VersionHistory

from zavod import settings
from zavod.archive.backend import get_archive_backend, ArchiveObject

if TYPE_CHECKING:
    from zavod.meta.dataset import Dataset

log = get_logger(__name__)
StatementGen = Generator[Statement, None, None]
STATEMENTS_FILE = "statements.pack"
ENTITIES_FILE = "entities.ftm.json"
STATISTICS_FILE = "statistics.json"
ISSUES_LOG = "issues.log"
ISSUES_FILE = "issues.json"
RESOURCES_FILE = "resources.json"
INDEX_FILE = "index.json"
CATALOG_FILE = "catalog.json"
HISTORY_FILE = "history.json"


def get_release_object(dataset_name: str, resource: str) -> ArchiveObject:
    backend = get_archive_backend()
    name = f"datasets/{settings.BACKFILL_RELEASE}/{dataset_name}/{resource}"
    return backend.get_object(name)


def backfill_resource(dataset_name: str, resource: str, path: Path) -> Optional[Path]:
    object = get_release_object(dataset_name, resource)
    if object.exists():
        log.info(
            "Backfilling dataset resource...",
            dataset=dataset_name,
            resource=resource,
            object=object.name,
        )
        object.backfill(path)
        return path
    return None


def publish_resource(
    path: Path,
    dataset_name: Optional[str],
    resource: str,
    latest: bool = True,
    mime_type: Optional[str] = None,
) -> None:
    backend = get_archive_backend()
    if dataset_name is not None:
        assert path.relative_to(dataset_data_path(dataset_name))
        resource = f"{dataset_name}/{resource}"
    release_name = f"datasets/{settings.RELEASE}/{resource}"
    release_object = backend.get_object(release_name)
    release_object.publish(path, mime_type=mime_type)

    if latest and settings.RELEASE != "latest":
        latest_name = f"datasets/latest/{resource}"
        latest_object = backend.get_object(latest_name)
        latest_object.republish(release_name)


def datasets_path() -> Path:
    return settings.DATA_PATH / "datasets"


def _state_path() -> Path:
    return settings.DATA_PATH / "state"


def dataset_data_path(dataset_name: str) -> Path:
    path = datasets_path() / dataset_name
    path.mkdir(parents=True, exist_ok=True)
    return path.resolve()


def dataset_state_path(dataset_name: str) -> Path:
    """The state directory is outside the main data directory and is used for temporary
    processing artifacts (like the materialised graph, and the timestamp index)."""
    path = _state_path() / dataset_name
    path.mkdir(parents=True, exist_ok=True)
    return path.resolve()


def clear_data_path(dataset_name: str) -> None:
    """Delete all recorded data for a given dataset."""
    shutil.rmtree(dataset_data_path(dataset_name), ignore_errors=True)
    shutil.rmtree(dataset_state_path(dataset_name), ignore_errors=True)


def dataset_resource_path(dataset_name: str, resource: str) -> Path:
    dataset_path = dataset_data_path(dataset_name)
    return dataset_path.joinpath(resource)


def get_dataset_resource(
    dataset: "Dataset",
    resource: str,
    backfill: bool = True,
    force_backfill: bool = False,
) -> Path:
    path = dataset_resource_path(dataset.name, resource)
    if path.exists() and not force_backfill:
        return path
    if backfill or force_backfill:
        backfill_resource(dataset.name, resource, path)
    return path


def get_dataset_index(dataset_name: str, backfill: bool = True) -> Optional[Path]:
    path: Optional[Path] = dataset_resource_path(dataset_name, INDEX_FILE)
    if path is not None and not path.exists() and backfill:
        path = backfill_resource(dataset_name, INDEX_FILE, path)
    if path is not None and path.exists():
        return path
    return None


@lru_cache(maxsize=500)
def get_dataset_history(dataset_name: str) -> VersionHistory:
    name = f"runs/{dataset_name}/{HISTORY_FILE}"
    backend = get_archive_backend()
    object = backend.get_object(name)
    if not object.exists():
        return VersionHistory([])
    data = object.open().read()
    return VersionHistory.from_json(data)


def get_run_object(
    dataset_name: str, resource: str, version: Optional[str] = None
) -> ArchiveObject:
    if version is None:
        history = get_dataset_history(dataset_name)
        if history.latest is not None:
            version = history.latest.id
        else:
            version = "NULL"
    backend = get_archive_backend()
    name = f"runs/{dataset_name}/{version}/{resource}"
    return backend.get_object(name)


def publish_dataset_history(dataset_name: str, version: Version) -> None:
    """Publish the history of runs for a given dataset to the data lake."""
    history = get_dataset_history(dataset_name)
    if version not in history.items:
        history = history.append(version)
    backend = get_archive_backend()
    path = dataset_resource_path(dataset_name, HISTORY_FILE)

    with open(path, "w") as fh:
        fh.write(history.to_json())

    name = f"runs/{dataset_name}/{HISTORY_FILE}"
    object = backend.get_object(name)
    object.publish(path, mime_type=JSON)
    name = f"runs/{dataset_name}/{version.id}/{HISTORY_FILE}"
    object = backend.get_object(name)
    object.publish(path, mime_type=JSON)


def publish_run_resource(
    path: Path,
    dataset_name: str,
    version: Version,
    resource: str,
    mime_type: Optional[str] = None,
) -> None:
    """Publish a file in the given run's directory of the given dataset."""
    name = f"runs/{dataset_name}/{version.id}/{resource}"
    backend = get_archive_backend()
    object = backend.get_object(name)
    object.publish(path, mime_type=mime_type)


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
        with open(path, "r") as fh:
            yield from _read_fh_statements(fh, external)
        return

    object = get_run_object(dataset.name, STATEMENTS_FILE)
    if not object.exists():
        object = get_release_object(dataset.name, STATEMENTS_FILE)
    if object.exists():
        log.info(
            "Streaming backfilled statements...",
            backfill_dataset=dataset.name,
            object=object.name,
        )
        with object.open() as fh:
            yield from _read_fh_statements(fh, external)
        return
    log.error(f"Cannot load statements for: {dataset.name}")


def iter_previous_statements(dataset: "Dataset", external: bool = True) -> StatementGen:
    """Load the statements from the previous release of the dataset by streaming them
    from the data archive."""
    for scope in dataset.leaves:
        object = get_run_object(dataset.name, STATEMENTS_FILE)
        if not object.exists():
            object = get_release_object(dataset.name, STATEMENTS_FILE)
        if object.exists():
            log.info(
                "Streaming backfilled statements...",
                dataset=scope.name,
                object=object.name,
            )
            with object.open() as fh:
                yield from _read_fh_statements(fh, external)
