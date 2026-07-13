from enum import StrEnum
import json
from typing import Any, Dict, List, Optional

from followthemoney.dataset import Version

from zavod import settings
from zavod.logs import get_logger
from zavod.meta import Dataset, get_catalog
from zavod.archive import INDEX_FILE, STATISTICS_FILE, ISSUES_FILE
from zavod.archive import CATALOG_FILE, DELTA_INDEX_FILE, DELTA_EXPORT_FILE
from zavod.archive import UNLISTED_RESOURCES
from zavod.archive import get_dataset_artifact, get_artifact_object
from zavod.archive import find_archive_artifact, latest_local_version
from zavod.archive import iter_dataset_versions, dataset_resource_path
from zavod.runtime.urls import make_artifact_url
from zavod.runtime.resources import DatasetResources
from zavod.runtime.issues import DatasetIssues
from zavod.util import write_json

log = get_logger(__name__)


class DatasetVersionResult(StrEnum):
    SUCCESS = "success"
    FAILURE = "failure"


def get_base_dataset_metadata(
    dataset: Dataset, version: Optional[Version]
) -> Dict[str, Any]:
    """Build the barebones metadata block for a dataset, without artifact URLs.

    Args:
        dataset: The dataset to generate metadata for.
        version: The run of the dataset to describe. Statistics and resources are
            read from exactly this version, so e.g. a failed run (which produced
            no statistics) never picks up entity counts from a previous run.
            `None` if the dataset has never run.
    """
    meta: Dict[str, Any] = {
        "issue_levels": {},
        "issue_count": 0,
        "updated_at": settings.RUN_TIME_ISO,
        "resources": [],
    }
    if version is None:
        return meta

    # This reads the file produced by the statistics exporter which
    # contains entity counts for the dataset, aggregated by various
    # criteria:
    statistics_path = get_dataset_artifact(dataset.name, version, STATISTICS_FILE)
    if statistics_path.is_file():
        with open(statistics_path, "r") as fh:
            stats: Dict[str, Any] = json.load(fh)
            meta["entity_count"] = stats.get("entity_count", 0)
            targets = stats.get("targets", {})
            meta["target_count"] = targets.get("total", 0)
            things = stats.get("things", {})
            meta["thing_count"] = things.get("total", 0)
            last_change = stats.get("last_change")
            if last_change is not None:
                meta["last_change"] = last_change

    res_datas: List[Dict[str, Any]] = []
    for res in DatasetResources(dataset, version).all():
        if res.name in UNLISTED_RESOURCES:
            continue
        res_data = res.model_dump(mode="json", exclude_none=True)
        res_data["path"] = res.name
        res_datas.append(res_data)
    meta["resources"] = res_datas
    return meta


def write_dataset_index(
    dataset: Dataset, version: Version, result: DatasetVersionResult
) -> None:
    """Export dataset metadata to index.json for the given run (version) of the
    dataset."""
    catalog = get_catalog()
    index_path = dataset_resource_path(dataset.name, version, INDEX_FILE)
    log.info(
        "Writing dataset index",
        path=index_path,
        version=version.id,
        is_collection=dataset.is_collection,
    )
    meta = get_base_dataset_metadata(dataset, version)
    meta.update(dataset.to_opensanctions_dict(catalog))

    # Remove redundant dataset hierarchy metadata
    # (see https://www.opensanctions.org/changelog/2/):
    meta.pop("externals", None)
    meta.pop("sources", None)
    meta.pop("collections", None)

    meta["version"] = version.id
    meta["updated_at"] = version.dt.isoformat()
    meta["index_url"] = make_artifact_url(dataset.name, version.id, INDEX_FILE)
    for res_data in meta["resources"]:
        res_data["url"] = make_artifact_url(dataset.name, version.id, res_data["path"])

    if not dataset.is_collection:
        issues = DatasetIssues(dataset, version)
        meta["issue_levels"] = issues.by_level()
        meta["issue_count"] = sum(meta["issue_levels"].values())
    meta["last_export"] = settings.RUN_TIME_ISO
    meta["result"] = result.value
    # NOTE: when adding a another URL here, make sure to update Delivery Service,
    # it has a static list of URLs to rewrite
    meta["issues_url"] = make_artifact_url(dataset.name, version.id, ISSUES_FILE)
    meta["statistics_url"] = make_artifact_url(
        dataset.name, version.id, STATISTICS_FILE
    )

    delta_index_path = dataset_resource_path(dataset.name, version, DELTA_INDEX_FILE)
    if delta_index_path.is_file():
        # Only generated for successful exports:
        meta["delta_url"] = make_artifact_url(
            dataset.name, version.id, DELTA_INDEX_FILE
        )
    else:
        # If the delta index is not available, try to find the newest delta index
        # generate the URL from that:
        for v in iter_dataset_versions(dataset.name):
            object = get_artifact_object(dataset.name, DELTA_EXPORT_FILE, v.id)
            if object is not None:
                meta["delta_url"] = make_artifact_url(
                    dataset.name, v.id, DELTA_INDEX_FILE
                )
                break

    with open(index_path, "wb") as fh:
        write_json(meta, fh)


def get_catalog_dataset(dataset: Dataset) -> Dict[str, Any]:
    """Get a metadata description of a single dataset for the catalog.

    Uses run information from the latest run available (a local run if present,
    otherwise the newest published index file in the archive), but patches it with
    the latest metadata from the dataset object to allow us to quickly patch the
    catalog without waiting for another export.
    """
    index_data: Optional[Dict[str, Any]] = None
    version = latest_local_version(dataset.name, with_resource=INDEX_FILE)
    if version is None:
        version, legacy_object = find_archive_artifact(dataset.name, INDEX_FILE)
        if version is None and legacy_object is not None:
            # FIXME: legacy fallback, remove after migration.
            with legacy_object.open() as fh:
                index_data = json.load(fh)
    if version is not None:
        path = get_dataset_artifact(dataset.name, version, INDEX_FILE)
        if path.is_file():
            with open(path, "r") as fh:
                index_data = json.load(fh)

    # Get a barebones metadata object, only relevant before the first export
    meta = get_base_dataset_metadata(dataset, version)

    if index_data is not None:
        meta.update(index_data)
    else:
        log.warn(
            "No index file found, dataset likely hasn't run yet",
            dataset=dataset.name,
            report_issue=False,
        )

    # Overwrite with latest metadata (without any run information), useful to quickly patch up the catalog
    # for datasets that don't get exported often.
    meta.update(dataset.to_opensanctions_dict(get_catalog()))

    return meta


def get_catalog_datasets(scope: Dataset) -> List[Dict[str, Any]]:
    datasets = []
    for dataset in scope.datasets:
        datasets.append(get_catalog_dataset(dataset))
    return datasets


def write_delta_index(
    dataset: Dataset,
    version: Version,
    max_versions: int = 100,
    include_latest: bool = True,
) -> None:
    """Export list of delta data versions for the dataset with their URLs
    associated."""
    versions: Dict[str, str] = {}

    # This hasn't been uploaded yet, but will become available at the same
    # time as the index file:
    if include_latest:
        data_path = dataset_resource_path(dataset.name, version, DELTA_EXPORT_FILE)
        if data_path.is_file() and data_path.stat().st_size > 0:
            versions[version.id] = make_artifact_url(
                dataset.name, version.id, DELTA_EXPORT_FILE
            )

    # Get the most recent versions of the dataset:
    for v in iter_dataset_versions(dataset.name):
        if v.id in versions:
            continue
        object = get_artifact_object(dataset.name, DELTA_EXPORT_FILE, v.id)
        if object is not None and object.size() > 0:
            versions[v.id] = make_artifact_url(dataset.name, v.id, DELTA_EXPORT_FILE)
        if len(versions) >= max_versions:
            break

    # Alternatively as a list for tooling that doesn't support iterating
    # over object keys https://github.com/opensanctions/opensanctions/issues/2216
    # Unstable key because we anticipate replacing this with functionality in
    # the upcoming data delivery service, so we don't want to suggest that this
    # is generally available.
    version_list = [
        {
            "version": version_id,
            "url": version_url,
        }
        for version_id, version_url in versions.items()
    ]

    if len(versions) == 0:
        log.info(f"No delta versions found: {dataset.name}")
        return
    index_path = dataset_resource_path(dataset.name, version, DELTA_INDEX_FILE)
    log.info("Writing delta versions index...", path=index_path.as_posix())
    with open(index_path, "wb") as fh:
        data = {
            "versions": versions,
            "unstable": {"version_list": version_list},
        }

        write_json(data, fh)


def write_catalog(scope: Dataset, version: Version) -> None:
    """Export a Nomenklatura-style data catalog file to represent all the datasets
    within this scope."""
    if not scope.is_collection:
        return
    catalog_path = dataset_resource_path(scope.name, version, CATALOG_FILE)
    log.info("Writing collection as catalog...", path=catalog_path.as_posix())
    with open(catalog_path, "wb") as fh:
        data = {
            "datasets": get_catalog_datasets(scope),
            "updated_at": settings.RUN_TIME_ISO,
        }
        write_json(data, fh)
