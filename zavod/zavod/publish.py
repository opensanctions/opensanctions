from rigour.mime.types import JSON
from followthemoney.dataset import Version, VersionHistory

from zavod.exporters.metadata import DatasetVersionResult
from zavod.meta import Dataset
from zavod.logs import get_logger
from zavod.archive import (
    INDEX_FILE,
    MANIFEST_FILE,
    dataset_artifact_directory,
    dataset_artifact_path,
    get_version_history,
)
from zavod.archive import dataset_resource_path
from zavod.archive import publish_version_history, archive_artifact
from zavod.archive import invalidate_dataset_urls
from zavod.archive import FAILURE_ARTIFACTS, VERSIONS_FILE
from zavod.runtime.resources import DatasetResources
from zavod.exporters import write_dataset_index

log = get_logger(__name__)


def _ensure_versions_file(
    dataset: Dataset, version: Version, success: bool = False
) -> None:
    """Ensure that the version history file exists for a dataset."""
    path = dataset_artifact_path(dataset.name, version, VERSIONS_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        history = get_version_history(dataset.name)
        with open(path, "w") as fh:
            fh.write(history.to_json())
    with open(path) as fh:
        history = VersionHistory.from_json(fh.read())
    if version not in history.items:
        history = history.append(version)
    if success:
        history.last_successful = version
    with open(path, "w") as fh:
        fh.write(history.to_json())


def publish_dataset(dataset: Dataset, version: Version) -> None:
    """Publish a successful dataset run:

    - Stamping this version as the last successful in the version history.
    - Uploading every file in the run's artifact directory to
      /artifacts/{dataset}/{version}/, then any registered resources not
      already covered from the dataset's resource folder.
    - Publishing the version history to the dataset's stable location.
    - Invalidating /datasets/latest/<dataset> and legacy /datasets/<date>/<dataset>
      URLs in CDN cache

    The upload order is deliberate: index.json describes the other artifacts,
    so it is uploaded only once all of them are in place, and the dataset's
    stable versions.json, which is what makes the version discoverable, is
    published last of all. A publish that dies part-way through therefore
    never leaves a discoverable version whose index points at missing files.
    """
    artifact_dir = dataset_artifact_directory(dataset.name, version)
    # Manifest.create writes this for every real run, source or collection:
    manifest_path = artifact_dir / MANIFEST_FILE
    assert manifest_path.is_file(), f"No run manifest for this version: {manifest_path}"
    # export_dataset writes this once every exporter has finished:
    index_path = artifact_dir / INDEX_FILE
    assert index_path.is_file(), f"No dataset index for this version: {index_path}"
    _ensure_versions_file(dataset, version, success=True)
    resources = {res.name: res for res in DatasetResources(dataset, version).all()}
    uploaded: set[str] = set()
    for path in sorted(artifact_dir.iterdir()):
        if not path.is_file() or path.name == INDEX_FILE:
            continue
        resource = resources.get(path.name)
        mime_type = resource.mime_type if resource is not None else None
        if mime_type is None and path.name.endswith(".json"):
            mime_type = JSON
        archive_artifact(path, dataset.name, version, path.name, mime_type=mime_type)
        uploaded.add(path.name)

    for name, resource in resources.items():
        if name in uploaded:
            continue
        path = dataset_resource_path(dataset.name, name)
        if not path.is_file():
            log.warning(
                "Registered resource not found",
                dataset=dataset.name,
                resource=name,
                version=version.id,
            )
            continue
        archive_artifact(
            path, dataset.name, version, name, mime_type=resource.mime_type
        )

    # Only after everything the index refers to has been archived:
    archive_artifact(index_path, dataset.name, version, INDEX_FILE, mime_type=JSON)
    # Only after the index is archived does the version become discoverable:
    publish_version_history(dataset.name, version)
    invalidate_dataset_urls(dataset.name, version)


def archive_failure(dataset: Dataset, version: Version) -> None:
    """Upload failure information about a dataset to the archive.

    Publishes only the artifacts in FAILURE_ARTIFACTS: the failure index and
    the issues that explain it, plus the version bookkeeping. Data files from
    the failed run stay local, and the version is registered in the history
    without becoming the last successful one.

    index.json is uploaded after the other artifacts and the stable versions.json
    last, so the failed version only becomes discoverable once everything it points
    at is archived."""
    _ensure_versions_file(dataset, version, success=False)
    write_dataset_index(dataset, version, DatasetVersionResult.FAILURE)
    for artifact in FAILURE_ARTIFACTS:
        if artifact == INDEX_FILE:
            continue
        path = dataset_artifact_path(dataset.name, version, artifact)
        if not path.is_file():
            continue
        archive_artifact(
            path,
            dataset.name,
            version,
            artifact,
            mime_type=JSON if artifact.endswith(".json") else None,
        )
    index_path = dataset_artifact_path(dataset.name, version, INDEX_FILE)
    archive_artifact(index_path, dataset.name, version, INDEX_FILE, mime_type=JSON)
    publish_version_history(dataset.name, version)
