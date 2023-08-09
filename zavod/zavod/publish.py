from zavod.meta import Dataset
from zavod.logs import get_logger
from zavod.archive import publish_resource, dataset_resource_path
from zavod.archive import INDEX_FILE, ISSUES_FILE, ISSUES_LOG
from zavod.archive import STATEMENTS_FILE, RESOURCES_FILE, STATISTICS_FILE
from zavod.runtime.resources import DatasetResources
from zavod.exporters import write_dataset_index, write_issues

log = get_logger(__name__)


def publish_dataset(dataset: Dataset, latest: bool = True):
    """Upload a dataset to the archive."""
    resources = DatasetResources(dataset)
    for resource in resources.all():
        path = dataset_resource_path(dataset.name, resource.name)
        if not path.is_file():
            log.error("Resource not found: %s" % path)
            continue
        publish_resource(
            path,
            dataset.name,
            resource.name,
            latest=latest,
            mime_type=resource.mime_type,
        )
    for meta in [STATEMENTS_FILE, ISSUES_FILE, ISSUES_LOG, RESOURCES_FILE, INDEX_FILE]:
        path = dataset_resource_path(dataset.name, meta)
        if not path.is_file():
            log.error("Metadata file not found: %s" % path)
            continue
        mime_type = "application/json" if meta.endswith(".json") else None
        publish_resource(path, dataset.name, meta, latest=latest, mime_type=mime_type)


def publish_failure(dataset: Dataset, latest: bool = True):
    """Upload failure information about a dataset to the archive."""
    # Clear out interim artifacts so they cannot pollute the metadata we're
    # generating.
    dataset_resource_path(dataset.name, RESOURCES_FILE).unlink(missing_ok=True)
    dataset_resource_path(dataset.name, STATEMENTS_FILE).unlink(missing_ok=True)
    dataset_resource_path(dataset.name, STATISTICS_FILE).unlink(missing_ok=True)
    dataset_resource_path(dataset.name, INDEX_FILE).unlink(missing_ok=True)
    write_issues(dataset)
    write_dataset_index(dataset)
    for meta in [ISSUES_FILE, ISSUES_LOG, INDEX_FILE]:
        path = dataset_resource_path(dataset.name, meta)
        if not path.is_file():
            log.error("Metadata file not found: %s" % path)
            continue
        mime_type = "application/json" if meta.endswith(".json") else None
        publish_resource(path, dataset.name, meta, latest=latest, mime_type=mime_type)
