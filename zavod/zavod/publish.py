from rigour.mime.types import JSON

from zavod.exporters.metadata import DatasetVersionResult
from zavod.meta import Dataset
from zavod.logs import get_logger
from zavod.archive import dataset_artifact_path, dataset_resource_path
from zavod.archive import publish_version_history, archive_artifact
from zavod.archive import invalidate_dataset_urls
from zavod.archive import INDEX_FILE, CATALOG_FILE
from zavod.archive import STATEMENTS_FILE, RESOURCES_FILE, STATISTICS_FILE
from zavod.archive import VERSIONS_FILE, EXTRA_ARTIFACTS, HASH_FILE
from zavod.archive import DELTA_EXPORT_FILE, DELTA_INDEX_FILE
from zavod.runtime.resources import DatasetResources
from zavod.runtime.versions import set_version_successful
from zavod.exporters import write_dataset_index

log = get_logger(__name__)


def _archive_artifacts(
    dataset: Dataset, version: str, extra_artifacts: list[str] = []
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


def publish_dataset(dataset: Dataset, version: str) -> None:
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


def archive_failure(dataset: Dataset, version: str) -> None:
    """Upload failure information about a dataset to the archive."""
    # For collections, we used to refuse to archive_failure because we were worried about a failed
    # `default/index.json` ending up at `/datasets/latest/default/index.json` with empty resources.
    # That's no longer a concern: we stopped publishing failed `index.json` to `/datasets` in
    # https://github.com/opensanctions/opensanctions/commit/476dcbc0088d5f92b9258244644e61754e85ffdb,
    # and `index.json` carries an explicit `result: failure` since
    # https://github.com/opensanctions/opensanctions/commit/ff9c602c66668393b66e79850fc1fb8810b899fa.
    # So archiving a failed collection just lands a `result: failure` version in `/artifacts`,
    # which is exactly what we want for surfacing the `issues.log`.
    # Clear out interim artifacts so they cannot pollute the metadata we're
    # generating. This deny-list is the sole guard against half-generated
    # export files reaching the archive.
    # TODO: invert this into an allow-list of failure artifacts (index.json,
    # issues.json, issues.log, versions.json) instead of unlinking everything
    # else.
    remove_artifacts = [
        STATEMENTS_FILE,
        STATISTICS_FILE,
        INDEX_FILE,
        CATALOG_FILE,
        RESOURCES_FILE,
        DELTA_EXPORT_FILE,
        DELTA_INDEX_FILE,
        HASH_FILE,
    ]
    for artifact in remove_artifacts:
        path = dataset_artifact_path(dataset.name, version, artifact)
        if path.is_file():
            path.unlink(missing_ok=True)

    write_dataset_index(dataset, version, DatasetVersionResult.FAILURE)
    path = dataset_artifact_path(dataset.name, version, INDEX_FILE)
    if not path.is_file():
        log.error(f"Metadata file not found: {path}", dataset=dataset.name)
        return
    _archive_artifacts(dataset, version)
    dataset_artifact_path(dataset.name, version, RESOURCES_FILE).unlink(missing_ok=True)
    dataset_artifact_path(dataset.name, version, VERSIONS_FILE).unlink(missing_ok=True)
