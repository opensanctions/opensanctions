from rigour.mime.types import JSON
from followthemoney.dataset import Version

from zavod.exporters.metadata import DatasetVersionResult
from zavod.meta import Dataset
from zavod.logs import get_logger
from zavod.archive import dataset_artifact_path, dataset_resource_path
from zavod.archive import publish_version_history, archive_artifact
from zavod.archive import invalidate_dataset_urls
from zavod.archive import CATALOG_FILE, EXTRA_ARTIFACTS, FAILURE_ARTIFACTS
from zavod.runtime.resources import DatasetResources
from zavod.runtime.versions import set_version_successful
from zavod.exporters import write_dataset_index

log = get_logger(__name__)


def _archive_artifacts(
    dataset: Dataset, version: Version, extra_artifacts: list[str] = []
) -> None:
    """
    Upload every file we persist about a run to /artifacts/{dataset}/{version}/.

    Also publishes the version history to the dataset's stable version history location.

    This covers both registered resources and non-resource files.
    """
    extra_artifacts = list(extra_artifacts) + EXTRA_ARTIFACTS

    for resource in DatasetResources(dataset, version).all():
        path = dataset_artifact_path(dataset.name, version, resource.name)
        if not path.is_file():
            path = dataset_resource_path(dataset.name, resource.name)
        if not path.is_file():
            log.error(f"Resource not found: {path}", dataset=dataset.name)
            continue
        archive_artifact(
            path,
            dataset.name,
            version,
            resource.name,
            mime_type=resource.mime_type,
        )

    for artifact in extra_artifacts:
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


def publish_dataset(dataset: Dataset, version: Version) -> None:
    """Publish a dataset.

    Only for successful runs.

    This entails

    - Adding this version to version history
    - Archiving all artifacts to /artifacts/{dataset}/{version}/
    - Stamping this version as the last successful in the version history.
    - Invalidating /datasets/latest/<dataset> and legacy /datasets/<date>/<dataset>
      URLs in CDN cache
    """
    set_version_successful(dataset, version)

    extra_artifacts = [CATALOG_FILE] if dataset.is_collection else []
    _archive_artifacts(dataset, version, extra_artifacts)

    invalidate_dataset_urls(dataset.name)


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
