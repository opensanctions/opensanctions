from rigour.mime.types import JSON

from zavod.meta import Dataset
from zavod.logs import get_logger
from zavod.archive import publish_resource, dataset_resource_path
from zavod.archive import publish_dataset_version, publish_artifact
from zavod.archive import INDEX_FILE, CATALOG_FILE
from zavod.archive import ARTIFACT_FILES
from zavod.runtime.resources import DatasetResources
from zavod.runtime.versions import get_latest
from zavod.exporters import write_dataset_index

log = get_logger(__name__)


def _archive_artifacts(dataset: Dataset) -> None:
    """Archive artifacts of the run to the data bucket."""
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


def publish_dataset(dataset: Dataset, latest: bool = True) -> None:
    """Publish a dataset to the archive.

    Note that we only publish successful runs to /datasets.
    Failed runs are only archived to /artifacts.
    """
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
            latest=latest,
            mime_type=resource.mime_type,
        )
    files = [INDEX_FILE]
    if dataset.is_collection:
        files.extend([CATALOG_FILE])
    for meta in files:
        path = dataset_resource_path(dataset.name, meta)
        if not path.is_file():
            log.error("Metadata file not found: %s" % path, dataset=dataset.name)
            continue
        mime_type = JSON if meta.endswith(".json") else None
        publish_resource(path, dataset.name, meta, latest=latest, mime_type=mime_type)
    _archive_artifacts(dataset)


def archive_failure(dataset: Dataset, latest: bool = True) -> None:
    """Archive artifacts about a run to the archive."""
    # Collections currently should never call publish_failure (as that only gets called for crawl and validate).
    # But if they ever did (for example to publish a failure in the export stage), we should think very well about
    # what exactly a failed index.json for default should look like. Currently, it would have empty resources,
    # and our clients likely wouldn't appreciate that.
    assert not dataset.is_collection

    # TODO(Leon Handreke): write a status: FAILED field to index
    # See https://github.com/opensanctions/opensanctions/issues/2541
    write_dataset_index(dataset)
    path = dataset_resource_path(dataset.name, INDEX_FILE)
    if not path.is_file():
        log.error("Metadata file not found: %s" % path, dataset=dataset.name)
        return

    # archive to /artifacts, but don't publish_resource anything to /datasets!
    # This is useful to debug failed runs.
    _archive_artifacts(dataset)
