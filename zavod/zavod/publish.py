from pathlib import Path
from typing import List, Optional, Tuple

from rigour.mime.types import JSON

from zavod.archive.backend import get_archive_backend
from zavod.exporters.metadata import DatasetVersionResult
from zavod.meta import Dataset
from zavod.logs import get_logger
from zavod.archive import DATASETS, LATEST, dataset_resource_path
from zavod.archive import publish_dataset_version, publish_artifact
from zavod.archive import republish_resource_from_artifact
from zavod.archive import INDEX_FILE, CATALOG_FILE
from zavod.archive import STATEMENTS_FILE, RESOURCES_FILE, STATISTICS_FILE
from zavod.archive import VERSIONS_FILE, ARTIFACT_FILES
from zavod.archive import DELTA_EXPORT_FILE, DELTA_INDEX_FILE
from zavod.runtime.resources import DatasetResources
from zavod.runtime.versions import get_latest
from zavod.exporters import write_dataset_index

log = get_logger(__name__)


def _archive_artifacts(dataset: Dataset) -> None:
    """Archive metadata-only artifacts (issues, statistics, versions, ...) to
    /artifacts/{dataset}/{version}/. Used by archive_failure; publish_dataset has
    its own inlined version that also handles the heavy resources."""
    version = get_latest(dataset.name, backfill=False)
    if version is None:
        raise ValueError(f"No working version found for dataset: {dataset.name}")
    for artifact in ARTIFACT_FILES:
        path = dataset_resource_path(dataset.name, artifact)
        if path.is_file():
            publish_artifact(
                path,
                dataset.name,
                version,
                artifact,
                mime_type=JSON if artifact.endswith(".json") else None,
            )
    publish_dataset_version(dataset.name)


def publish_dataset(dataset: Dataset, republish_to_latest: bool = True) -> None:
    """Publish a dataset.

    Each per-dataset file is uploaded once to /artifacts/{dataset}/{version}/{file}
    (the canonical, immutable URL surfaced in metadata) and then server-side copied
    to /datasets/{RELEASE}/{dataset}/{file} (and /datasets/{LATEST}/{dataset}/{file})
    for back-compat with customers who hardcode those URLs. Internal-only metadata
    artifacts (issues, statistics, versions, ...) only go to /artifacts/.
    """
    version = get_latest(dataset.name, backfill=False)
    if version is None:
        raise ValueError(f"No working version found for dataset: {dataset.name}")

    # (path, name, mime_type) for files that are both archived as artifacts AND
    # copied to the /datasets/ paths.
    resource_files: List[Tuple[Path, str, Optional[str]]] = []
    resources = DatasetResources(dataset)
    for resource in resources.all():
        if resource.name in ARTIFACT_FILES:
            # The delta and statistics exporters write internal artifacts via the
            # resources registry; strip them so they don't appear in the
            # dataset's public resource list.
            resources.remove(resource.name)
            continue
        path = dataset_resource_path(dataset.name, resource.name)
        if not path.is_file():
            log.error("Resource not found: %s" % path, dataset=dataset.name)
            continue
        resource_files.append((path, resource.name, resource.mime_type))

    meta_files = [INDEX_FILE]
    if dataset.is_collection:
        meta_files.append(CATALOG_FILE)
    for meta in meta_files:
        path = dataset_resource_path(dataset.name, meta)
        if not path.is_file():
            log.error("Metadata file not found: %s" % path, dataset=dataset.name)
            continue
        mime_type = JSON if meta.endswith(".json") else None
        resource_files.append((path, meta, mime_type))

    # 1. Upload each resource/index/catalog file to /artifacts/{dataset}/{version}/.
    for path, name, mime_type in resource_files:
        publish_artifact(path, dataset.name, version, name, mime_type=mime_type)

    # 2. Upload the remaining internal artifacts (issues, statistics, versions,
    #    delta, hash, resources) to /artifacts/ only — they don't get copied to
    #    /datasets/.
    for artifact in ARTIFACT_FILES:
        if artifact in (INDEX_FILE, CATALOG_FILE):
            continue
        path = dataset_resource_path(dataset.name, artifact)
        if path.is_file():
            publish_artifact(
                path,
                dataset.name,
                version,
                artifact,
                mime_type=JSON if artifact.endswith(".json") else None,
            )
    publish_dataset_version(dataset.name)

    # 3. Server-side copy each public file from /artifacts/ into the
    #    /datasets/{RELEASE}/ and /datasets/{LATEST}/ locations.
    if republish_to_latest:
        published_files = {name for _path, name, _mime_type in resource_files}
        _warn_about_stale_latest_files(dataset, published_files)
    for _path, name, _mime_type in resource_files:
        republish_resource_from_artifact(
            dataset.name, version.id, name, republish_to_latest=republish_to_latest
        )


def _warn_about_stale_latest_files(
    dataset: Dataset, published_files: set[str]
) -> None:
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
    # Collections currently should never call publish_failure (as that only gets called for crawl and validate).
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
