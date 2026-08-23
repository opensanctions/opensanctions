from rigour.mime.types import JSON
from followthemoney.dataset import Version, VersionHistory

from zavod.exporters.metadata import DatasetVersionResult
from zavod.meta import Dataset
from zavod.logs import get_logger
from zavod.archive import dataset_artifact_directory, dataset_artifact_path
from zavod.archive import dataset_resource_path
from zavod.archive import publish_version_history, archive_artifact
from zavod.archive import invalidate_dataset_urls
from zavod.archive import FAILURE_ARTIFACTS, VERSIONS_FILE
from zavod.runtime.resources import DatasetResources
from zavod.exporters import write_dataset_index

log = get_logger(__name__)


def publish_dataset(dataset: Dataset, version: Version) -> None:
    """Publish a successful dataset run:

    - Stamping this version as the last successful in the version history.
    - Uploading every file in the run's artifact directory to
      /artifacts/{dataset}/{version}/, then any registered resources not
      already covered from the dataset's resource folder.
    - Publishing the version history to the dataset's stable location.
    - Invalidating /datasets/latest/<dataset> and legacy /datasets/<date>/<dataset>
      URLs in CDN cache
    """
    path = dataset_artifact_path(dataset.name, version, VERSIONS_FILE)
    if not path.exists():
        raise RuntimeError(
            f"Version history file does not exist for dataset {dataset.name}"
        )
    with open(path) as fh:
        history = VersionHistory.from_json(fh.read())
    if version not in history.items:
        history.items.append(version)
    history.last_successful = version
    with open(path, "w") as fh:
        fh.write(history.to_json())

    resources = {res.name: res for res in DatasetResources(dataset, version).all()}
    uploaded: set[str] = set()
    for path in sorted(dataset_artifact_directory(dataset.name, version).iterdir()):
        if not path.is_file():
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

    publish_version_history(dataset.name, version)
    invalidate_dataset_urls(dataset.name, version)


def archive_failure(dataset: Dataset, version: Version) -> None:
    """Upload failure information about a dataset to the archive.

    Publishes only the artifacts in FAILURE_ARTIFACTS: the failure index and
    the issues that explain it, plus the version bookkeeping. Data files from
    the failed run stay local, and the version is registered in the history
    without becoming the last successful one."""
    write_dataset_index(dataset, version, DatasetVersionResult.FAILURE)
    for artifact in FAILURE_ARTIFACTS:
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
    publish_version_history(dataset.name, version)
