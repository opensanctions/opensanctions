from typing import List

from rigour.mime.types import JSON
from followthemoney.dataset import Version

from zavod.archive.backend import get_archive_backend
from zavod.exporters.metadata import DatasetVersionResult
from zavod.meta import Dataset
from zavod.logs import get_logger
from zavod.archive import DATASETS, LATEST, UNLISTED_RESOURCES, dataset_resource_path
from zavod.archive import publish_version_history, archive_artifact
from zavod.archive import publish_artifact, get_dataset_artifact
from zavod.archive import INDEX_FILE, CATALOG_FILE
from zavod.archive import STATEMENTS_FILE, RESOURCES_FILE, STATISTICS_FILE
from zavod.archive import VERSIONS_FILE, EXTRA_ARTIFACTS
from zavod.archive import DELTA_EXPORT_FILE, DELTA_INDEX_FILE
from zavod.runtime.resources import DatasetResources
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
        path = dataset_resource_path(dataset.name, version, resource.name)
        if not path.is_file():
            log.error("Resource not found: %s" % path, dataset=dataset.name)
            continue
        archive_artifact(
            path,
            dataset.name,
            version,
            resource.name,
            mime_type=resource.mime_type,
        )

    for artifact in extra_artifacts:
        path = dataset_resource_path(dataset.name, version, artifact)
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


def publish_dataset(
    dataset: Dataset, version: Version, republish_to_latest: bool = True
) -> None:
    """Publish the given run (version) of a dataset.

    Every file we persist about this run is uploaded to /artifacts/{dataset}/{version}/

    Listed resources plus index and collection catalog are copied to
    /datasets/{RELEASE}/{dataset}/ backward compatibility and
    /datasets/{LATEST}/{dataset}/ for discovery without the full catalog.
    """
    # Make sure the version history file of this run is available locally, e.g.
    # when publishing a version that was crawled on another machine.
    versions_path = get_dataset_artifact(dataset.name, version, VERSIONS_FILE)
    if not versions_path.is_file():
        raise ValueError(
            f"No version history found for {dataset.name} at version {version.id}"
        )

    extra_artifacts = []
    all_published_files: List[str] = [
        r.name
        for r in DatasetResources(dataset, version).all()
        if r.name not in UNLISTED_RESOURCES
    ]
    all_published_files.append(INDEX_FILE)

    if dataset.is_collection:
        extra_artifacts.append(CATALOG_FILE)
        all_published_files.append(CATALOG_FILE)

    _archive_artifacts(dataset, version, extra_artifacts)

    if republish_to_latest:
        _warn_about_stale_latest_files(dataset, set(all_published_files))

    for name in all_published_files:
        publish_artifact(dataset.name, version.id, name, republish_to_latest)


def _warn_about_stale_latest_files(dataset: Dataset, published_files: set[str]) -> None:
    """Warn about files in datasets/latest/<dataset>/ that we no longer publish
    so we can clean them up by hand. We don't delete automatically because
    deleting from the bucket is scary."""
    backend = get_archive_backend()
    latest_prefix = f"{DATASETS}/{LATEST}/{dataset.name}/"
    for object in backend.list_objects(latest_prefix):
        basename = object.name[len(latest_prefix) :]
        if basename not in published_files:
            log.warning(
                f"Stale file in datasets/latest/{dataset.name}: {basename}",
                dataset=dataset.name,
                object=object.name,
                updated_at=object.updated_at().isoformat(),
            )


def archive_failure(dataset: Dataset, version: Version) -> None:
    """Upload failure information about the given run (version) of a dataset to
    the archive."""
    # For collections, we used to refuse to archive_failure because we were worried about a failed
    # `default/index.json` ending up at `/datasets/latest/default/index.json` with empty resources.
    # That's no longer a concern: we stopped publishing failed `index.json` to `/datasets` in
    # https://github.com/opensanctions/opensanctions/commit/476dcbc0088d5f92b9258244644e61754e85ffdb,
    # and `index.json` carries an explicit `result: failure` since
    # https://github.com/opensanctions/opensanctions/commit/ff9c602c66668393b66e79850fc1fb8810b899fa.
    # So archiving a failed collection just lands a `result: failure` version in `/artifacts`,
    # which is exactly what we want for surfacing the `issues.log`.
    # Clear out interim artifacts so they cannot pollute the metadata we're
    # generating.
    dataset_resource_path(dataset.name, version, STATEMENTS_FILE).unlink(
        missing_ok=True
    )
    # The statistics file may have been written by a partially-completed export, so
    # its counts wouldn't describe anything a consumer can download. Because the
    # index is generated from exactly this version, clearing it also guarantees the
    # failure index carries no entity counts from a previous run.
    dataset_resource_path(dataset.name, version, STATISTICS_FILE).unlink(
        missing_ok=True
    )
    dataset_resource_path(dataset.name, version, INDEX_FILE).unlink(missing_ok=True)
    dataset_resource_path(dataset.name, version, CATALOG_FILE).unlink(missing_ok=True)
    dataset_resource_path(dataset.name, version, RESOURCES_FILE).unlink(missing_ok=True)
    dataset_resource_path(dataset.name, version, DELTA_EXPORT_FILE).unlink(
        missing_ok=True
    )
    dataset_resource_path(dataset.name, version, DELTA_INDEX_FILE).unlink(
        missing_ok=True
    )

    write_dataset_index(dataset, version, DatasetVersionResult.FAILURE)
    path = dataset_resource_path(dataset.name, version, INDEX_FILE)
    if not path.is_file():
        log.error("Metadata file not found: %s" % path, dataset=dataset.name)
        return
    _archive_artifacts(dataset, version)
