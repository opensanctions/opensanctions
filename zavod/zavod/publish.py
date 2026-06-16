from rigour.mime.types import JSON

from zavod.archive.backend import get_archive_backend
from zavod.exporters.metadata import DatasetVersionResult
from zavod.meta import Dataset
from zavod.logs import get_logger
from zavod.archive import DATASETS, LATEST, publish_resource, dataset_resource_path
from zavod.archive import publish_dataset_version, publish_artifact
from zavod.archive import INDEX_FILE, CATALOG_FILE
from zavod.archive import STATEMENTS_FILE, RESOURCES_FILE, STATISTICS_FILE
from zavod.archive import VERSIONS_FILE, ARTIFACT_FILES
from zavod.archive import DELTA_EXPORT_FILE, DELTA_INDEX_FILE
from zavod.runtime.resources import DatasetResources
from zavod.runtime.versions import get_latest
from zavod.exporters import write_dataset_index

log = get_logger(__name__)


def _archive_artifacts(dataset: Dataset) -> None:
    """Archive artifacts to the /artifacts/ path on the data bucket."""
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
    """Publish a dataset to the archive, i.e. to /datasets."""
    resources = DatasetResources(dataset)
    for resource in resources.all():
        if resource.name in ARTIFACT_FILES:
            # This is a bit hacky: the delta exporter and statistics exporter are
            # generating artifacts used for internal purposes, but they should
            # not be included in the dataset metadata.
            resources.remove(resource.name)
            continue
        path = dataset_resource_path(dataset.name, resource.name)
        if not path.is_file():
            log.error("Resource not found: %s" % path, dataset=dataset.name)
            continue
        publish_resource(
            path,
            dataset.name,
            resource.name,
            republish_to_latest=republish_to_latest,
            mime_type=resource.mime_type,
        )
    meta_files = [INDEX_FILE]
    if dataset.is_collection:
        meta_files.extend([CATALOG_FILE])
    for meta_file in meta_files:
        path = dataset_resource_path(dataset.name, meta_file)
        if not path.is_file():
            log.error("Metadata file not found: %s" % path, dataset=dataset.name)
            continue
        mime_type = JSON if meta_file.endswith(".json") else None
        publish_resource(
            path,
            dataset.name,
            meta_file,
            republish_to_latest=republish_to_latest,
            mime_type=mime_type,
        )

    if republish_to_latest:
        all_published_files = set(meta_files) | {r.name for r in resources.all()}
        _warn_about_stale_latest_files(dataset, all_published_files)

    _archive_artifacts(dataset)


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
