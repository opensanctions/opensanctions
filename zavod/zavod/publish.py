from typing import List

from rigour.mime.types import JSON

from zavod.archive.backend import get_archive_backend
from zavod.exporters.metadata import DatasetVersionResult
from zavod.meta import Dataset
from zavod.logs import get_logger
from zavod.archive import DATASETS, LATEST, UNLISTED_RESOURCES, dataset_resource_path
from zavod.archive import publish_version_history, archive_artifact
from zavod.archive import publish_artifact
from zavod.archive import INDEX_FILE, CATALOG_FILE
from zavod.archive import STATEMENTS_FILE, RESOURCES_FILE, STATISTICS_FILE
from zavod.archive import VERSIONS_FILE, EXTRA_ARTIFACTS
from zavod.archive import DELTA_EXPORT_FILE, DELTA_INDEX_FILE
from zavod.runtime.resources import DatasetResources
from zavod.runtime.versions import get_latest
from zavod.exporters import write_dataset_index

log = get_logger(__name__)


def _archive_artifacts(dataset: Dataset, extra_artifacts: list[str] = []) -> None:
    """
    Upload every file we persist about a run to /artifacts/{dataset}/{version}/.

    Also publishes the version history to the dataset's stable version history location.

    This covers both registered resources and non-resource files.
    """
    extra_artifacts = list(extra_artifacts) + EXTRA_ARTIFACTS

    version = get_latest(dataset.name, backfill=False)
    if version is None:
        raise ValueError(f"No working version found for dataset: {dataset.name}")

    for resource in DatasetResources(dataset).all():
        path = dataset_resource_path(dataset.name, resource.name)
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
        path = dataset_resource_path(dataset.name, artifact)
        if not path.is_file():
            continue
        archive_artifact(
            path,
            dataset.name,
            version,
            artifact,
            mime_type=JSON if artifact.endswith(".json") else None,
        )

    publish_version_history(dataset.name)


def publish_dataset(dataset: Dataset, republish_to_latest: bool = True) -> None:
    """Publish a dataset.

    Every file we persist about this run is uploaded to /artifacts/{dataset}/{version}/

    Listed resources plus index and collection catalog are copied to
    /datasets/{RELEASE}/{dataset}/ backward compatibility and
    /datasets/{LATEST}/{dataset}/ for discovery without the full catalog.
    """

    extra_artifacts = []
    all_published_files: List[str] = [
        r.name
        for r in DatasetResources(dataset).all()
        if r.name not in UNLISTED_RESOURCES
    ]
    all_published_files.append(INDEX_FILE)

    if dataset.is_collection:
        extra_artifacts.append(CATALOG_FILE)
        all_published_files.append(CATALOG_FILE)

    _archive_artifacts(dataset, extra_artifacts)

    version = get_latest(dataset.name, backfill=False)
    assert version is not None

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


def archive_failure(dataset: Dataset) -> None:
    """Upload failure information about a dataset to the archive."""
    # Collections currently should never call archive_failure (as that only gets called for crawl and validate).
    # But if they ever did (for example to publish a failure in the export stage), we should think very well about
    # what exactly a failed index.json for default should look like. Currently, it would have empty resources,
    # and our clients likely wouldn't appreciate that.
    assert not dataset.is_collection
    # Clear out interim artifacts so they cannot pollute the metadata we're
    # generating.
    dataset_resource_path(dataset.name, STATEMENTS_FILE).unlink(missing_ok=True)
    # TODO: The statistics file gets pulled in by write_dataset_index,
    #  so they get published as part of the artifacts anyway.
    #  For a brief discussion of our currently broken failure semantics,
    #  see https://github.com/opensanctions/opensanctions/pull/2483
    dataset_resource_path(dataset.name, STATISTICS_FILE).unlink(missing_ok=True)
    dataset_resource_path(dataset.name, INDEX_FILE).unlink(missing_ok=True)
    dataset_resource_path(dataset.name, CATALOG_FILE).unlink(missing_ok=True)
    dataset_resource_path(dataset.name, RESOURCES_FILE).unlink(missing_ok=True)
    dataset_resource_path(dataset.name, DELTA_EXPORT_FILE).unlink(missing_ok=True)
    dataset_resource_path(dataset.name, DELTA_INDEX_FILE).unlink(missing_ok=True)

    write_dataset_index(dataset, DatasetVersionResult.FAILURE)
    path = dataset_resource_path(dataset.name, INDEX_FILE)
    if not path.is_file():
        log.error("Metadata file not found: %s" % path, dataset=dataset.name)
        return
    _archive_artifacts(dataset)
    dataset_resource_path(dataset.name, RESOURCES_FILE).unlink(missing_ok=True)
    dataset_resource_path(dataset.name, VERSIONS_FILE).unlink(missing_ok=True)
