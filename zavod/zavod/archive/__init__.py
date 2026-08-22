"""
The archive is the place where we store the outputs of zavod runs
beyond their local scratch space.

See archive backends for operating on the archive - in OpenSanctions production
this is the Google Cloud Storage bucket served at
https://data.opensanctions.org. A local filesystem path can be used in
development and testing.

Layout
------

`/artifacts/{dataset}/{version}/` is the canonical, immutable location for all
outputs of a given run, and is what we point to in the metadata. It holds both
the listed resources (e.g. `entities.ftm.json`) and run artifacts such as
`index.json`, `statistics.json`, `issues.json`, `statements.pack`,
`entities.delta.json`, `delta.json` and a `versions.json` snapshot.

`/artifacts/{dataset}/versions.json` is the root version file: a window of the
most recent version IDs of the dataset (oldest first, up to
`VersionHistory.LENGTH`) plus the ID of the last successful run, e.g.
`{"items": ["20260629141001-mek", ...], "last_successful": "20260707123218-hai"}`.
It is updated on every run, including failed ones.

`/datasets/{date_stamp}/{dataset}/` is the legacy date-stamped location for
metadata and resources. We redirect HTTP requests for dates >= 2026-08-17
to the corresponding `/artifacts/` version directory. When in use, subsequent
runs starting on the same day overwrote files from earlier runs.

`/datasets/latest/{dataset}/` was where the latest successful run would copy
its outputs. We now redirect requests to the latest successful version in `/artifacts/`.

Walking versions
----------------

Version IDs (see `followthemoney.dataset.versions.Version`) are opaque
strings, but they sort chronologically. The root
version file only holds a bounded window, but every run's artifact directory
contains a `versions.json` snapshot whose window ends at that version. To walk
the full history: read the root version file, iterate its items newest-first,
then fetch `/artifacts/{dataset}/{oldest_item}/versions.json` and repeat until
the window no longer extends further back. This is implemented in
`iter_dataset_versions()`, which needs a configured archive backend.

To inspect the run history of a production dataset without one, use the
maintenance tool, which walks the same snapshots over plain HTTPS and
tabulates each run's `index.json`:

    python -m contrib.maintenance.versions <dataset_name>

Success and failure
-------------------

Each run's `index.json` has a `result` field, either "success" or "failure".
Failed runs archive their index.json and issues files if possible and log the
new version in versions.json, but do not archive data files or update the last
successful version reference.
"""

import shutil
from pathlib import Path
from functools import lru_cache
from typing import TYPE_CHECKING
from typing import TextIO
from collections.abc import Generator
from rigour.mime.types import JSON
from followthemoney import Statement
from followthemoney.statement.serialize import read_pack_statements_decoded
from followthemoney.dataset import Version, VersionHistory

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
LATEST = "latest"
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
# HACK: DatasetResources are defined as downloadable files of a dataset.
# A couple of exporters use this as a mechanism to get files archived,
# but their files are listed elsewhere in the dataset metadata so we don't
# want them duplicated in the resources section of the metadata.
UNLISTED_RESOURCES = [
    STATISTICS_FILE,
    DELTA_EXPORT_FILE,
]
# Files we persist about a run other than DatasetResources.
EXTRA_ARTIFACTS = [
    ISSUES_FILE,
    ISSUES_LOG,
    INDEX_FILE,
    STATEMENTS_FILE,
    VERSIONS_FILE,
    RESOURCES_FILE,
    HASH_FILE,
    DELTA_INDEX_FILE,
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
    """Downloaded resources, not subject to versioning."""
    dataset_path = dataset_data_path(dataset_name)
    return dataset_path.joinpath(resource)


def dataset_artifact_path(dataset_name: str, version: str, artifact: str) -> Path:
    """Versioned artifacts."""
    dataset_path = dataset_data_path(dataset_name)
    artifact_path = dataset_path / "_artifacts" / version
    artifact_path.mkdir(parents=True, exist_ok=True)
    return artifact_path / artifact


@lru_cache(maxsize=5000)
def get_versions_data(dataset_name: str, version: str | None = None) -> str | None:
    """Fetch the latest version data from the artifact base directory."""
    backend = get_archive_backend()
    name = f"{ARTIFACTS}/{dataset_name}/{VERSIONS_FILE}"
    if version is not None:
        name = f"{ARTIFACTS}/{dataset_name}/{version}/{VERSIONS_FILE}"
    object = backend.get_object(name)
    if object.exists():
        return object.open().read()
    return None


@lru_cache(maxsize=5000)
def get_last_successful_version(dataset_name: str) -> str | None:
    """Get the last successful version of a dataset, ie. the last one which produced
    a set of artifacts that were archived. Change detection and delta generation are
    based on this version."""
    data = get_versions_data(dataset_name)
    if data is None:
        return None
    history = VersionHistory.from_json(data)
    if history.last_successful:
        return history.last_successful.id
    return None


def iter_dataset_versions(dataset_name: str) -> Generator[Version, None, None]:
    """Iterate over all versions of a given dataset."""
    data = get_versions_data(dataset_name)
    seen: set[str] = set()
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
    dataset_name: str, version: str, resource: str
) -> ArchiveObject | None:
    backend = get_archive_backend()
    name = f"{ARTIFACTS}/{dataset_name}/{version}/{resource}"
    object = backend.get_object(name)
    if object.exists():
        return object
    return None


def publish_version_history(dataset_name: str, version: str) -> None:
    """Publish the history of versions for a given dataset to the artifact directory."""
    path = dataset_artifact_path(dataset_name, version, VERSIONS_FILE)
    if not path.exists():
        raise RuntimeError(f"Version history not found: {dataset_name}")

    backend = get_archive_backend()
    name = f"{ARTIFACTS}/{dataset_name}/{VERSIONS_FILE}"
    object = backend.get_object(name)
    object.publish(path, mime_type=JSON, ttl=TTL_SHORT)
    invalidate_archive_cache(name)
    get_versions_data.cache_clear()


def archive_artifact(
    path: Path,
    dataset_name: str,
    version: str,
    artifact: str,
    mime_type: str | None = None,
) -> None:
    """Publish a file in the given versions artifact directory of the dataset."""
    assert path.relative_to(dataset_data_path(dataset_name))
    name = f"{ARTIFACTS}/{dataset_name}/{version}/{artifact}"
    backend = get_archive_backend()
    object = backend.get_object(name)
    object.publish(path, mime_type=mime_type, ttl=TTL_LONG)


def invalidate_dataset_urls(dataset_name: str) -> None:
    """Purge the CDN cache for a dataset's date-stamped and '/latest/' URLs
    under /datasets/."""
    release_prefix = f"{DATASETS}/{settings.RELEASE}/{dataset_name}/*"
    invalidate_archive_cache(release_prefix)
    latest_prefix = f"{DATASETS}/{LATEST}/{dataset_name}/*"
    invalidate_archive_cache(latest_prefix)


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
    if not path.exists():
        raise FileNotFoundError(f"Statements not found: {dataset.name}")
    with open(path) as fh:
        yield from _read_fh_statements(fh, external)


def _iter_scope_statements(dataset: "Dataset", external: bool = True) -> StatementGen:
    try:
        yield from iter_local_statements(dataset, external=external)
        return
    except FileNotFoundError:
        pass

    version = get_last_successful_version(dataset.name)
    if version is None:
        log.warning(f"No last successful version found for: {dataset.name}")
        return

    yield from stream_statements(dataset, version, external=external)


def stream_statements(
    dataset: "Dataset", version: str, external: bool = True
) -> StatementGen:
    """Load the statements from the previous release of the dataset by streaming them
    from the data archive."""
    if dataset.is_collection:
        raise TypeError(f"Cannot stream collection: {dataset.name}")
    object = get_artifact_object(dataset.name, version, STATEMENTS_FILE)
    if object is None:
        log.error(f"Cannot load statements for: {dataset.name}")
        return
    log.info(
        "Streaming statements...",
        current=dataset.name,
        object=object.name,
    )
    with object.open() as fh:
        yield from _read_fh_statements(fh, external)
