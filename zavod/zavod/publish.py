from rigour.mime.types import JSON

from zavod.meta import Dataset
from zavod.logs import get_logger
from zavod.archive import publish_resource, dataset_resource_path
from zavod.archive import publish_dataset_version, publish_artifact
from zavod.archive import INDEX_FILE, CATALOG_FILE
from zavod.archive import STATEMENTS_FILE, RESOURCES_FILE, STATISTICS_FILE
from zavod.archive import VERSIONS_FILE, ARTIFACT_FILES
from zavod.archive import DELTA_EXPORT_FILE, DELTA_INDEX_FILE
from zavod.runtime.resources import DatasetResources
from zavod.runtime.versions import get_latest
from zavod.exporters import write_dataset_index

log = get_logger(__name__)


def _publish_artifacts(dataset: Dataset) -> None:
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
    """Upload a dataset to the archive."""
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
    _publish_artifacts(dataset)


def publish_failure(dataset: Dataset, latest: bool = True) -> None:
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

    write_dataset_index(dataset)
    path = dataset_resource_path(dataset.name, INDEX_FILE)
    if not path.is_file():
        log.error("Metadata file not found: %s" % path, dataset=dataset.name)
        return
    publish_resource(path, dataset.name, INDEX_FILE, latest=latest, mime_type=JSON)
    _publish_artifacts(dataset)
    dataset_resource_path(dataset.name, RESOURCES_FILE).unlink(missing_ok=True)
    dataset_resource_path(dataset.name, VERSIONS_FILE).unlink(missing_ok=True)
