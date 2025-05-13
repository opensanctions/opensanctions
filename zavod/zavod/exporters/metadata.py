import json
from typing import Any, Dict, List, Optional
from nomenklatura.versions import Version

from zavod import settings
from zavod.logs import get_logger
from zavod.meta import Dataset, get_catalog
from zavod.archive import INDEX_FILE, STATISTICS_FILE, ISSUES_FILE
from zavod.archive import CATALOG_FILE, DELTA_INDEX_FILE, DELTA_EXPORT_FILE
from zavod.archive import ARTIFACT_FILES
from zavod.archive import get_dataset_artifact, get_artifact_object
from zavod.archive import iter_dataset_versions, dataset_resource_path
from zavod.runtime.urls import make_published_url, make_artifact_url
from zavod.runtime.resources import DatasetResources
from zavod.runtime.issues import DatasetIssues, Issue
from zavod.runtime.versions import get_latest
from zavod.util import write_json

log = get_logger(__name__)


def get_base_dataset_metadata(
    dataset: Dataset, version: Optional[Version] = None
) -> Dict[str, Any]:
    meta = {
        "issue_levels": {},
        "issue_count": 0,
        "updated_at": settings.RUN_TIME_ISO,
        "index_url": make_published_url(dataset.name, INDEX_FILE),
    }
    if version is not None:
        meta["version"] = version.id
        meta["updated_at"] = version.dt.isoformat()

    # This reads the file produced by the statistics exporter which
    # contains entity counts for the dataset, aggregated by various
    # criteria:
    statistics_path = get_dataset_artifact(dataset.name, STATISTICS_FILE)
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

    resources = DatasetResources(dataset)
    res_datas: List[Dict[str, Any]] = []
    for res in resources.all():
        if res.name in ARTIFACT_FILES:
            # TODO: we could make artifact URLs here?
            continue
        res_data = res.to_opensanctions_dict()
        if version is not None:
            # Attach a cache-busting version identifier to the resource URLs:
            url = res_data.get("url")
            if url is not None and "?" not in url:
                res_data["url"] = f"{url}?v={version.id}"
        res_datas.append(res_data)
    meta["resources"] = res_datas
    return meta


def write_dataset_index(dataset: Dataset) -> None:
    """Export dataset metadata to index.json."""
    catalog = get_catalog()
    version = get_latest(dataset.name, backfill=True)
    if version is None:
        raise ValueError(f"No version found for dataset: {dataset.name}")
    index_path = dataset_resource_path(dataset.name, INDEX_FILE)
    log.info(
        "Writing dataset index",
        path=index_path,
        version=version.id,
        is_collection=dataset.is_collection,
    )
    meta = get_base_dataset_metadata(dataset, version=version)
    meta.update(dataset.to_opensanctions_dict(catalog))

    # Remove redundant dataset hierarchy metadata
    # (see https://www.opensanctions.org/changelog/2/):
    meta.pop("externals", None)
    meta.pop("sources", None)
    meta.pop("collections", None)

    if not dataset.is_collection:
        issues = DatasetIssues(dataset)
        meta["issue_levels"] = issues.by_level()
        meta["issue_count"] = sum(meta["issue_levels"].values())
    meta["last_export"] = settings.RUN_TIME_ISO
    meta["issues_url"] = make_artifact_url(dataset.name, version.id, ISSUES_FILE)
    meta["statistics_url"] = make_artifact_url(
        dataset.name, version.id, STATISTICS_FILE
    )

    delta_index_path = dataset_resource_path(dataset.name, DELTA_INDEX_FILE)
    if delta_index_path.is_file():
        # Only generated for successful exports:
        meta["delta_url"] = make_artifact_url(
            dataset.name, version.id, DELTA_INDEX_FILE
        )
    else:
        # If the delta index is not available, try to find the newest delta index
        # generate the URL from that:
        for version in iter_dataset_versions(dataset.name):
            object = get_artifact_object(
                dataset.name, DELTA_EXPORT_FILE, version=version.id
            )
            if object is not None:
                meta["delta_url"] = make_artifact_url(
                    dataset.name, version.id, DELTA_INDEX_FILE
                )
                break

    with open(index_path, "wb") as fh:
        write_json(meta, fh)


def get_catalog_dataset(dataset: Dataset) -> Dict[str, Any]:
    """Get a metadata description of a single dataset, retaining timestamp information
    for the last export, but updating some other metadata."""
    catalog = get_catalog()
    meta = get_base_dataset_metadata(dataset)
    path = get_dataset_artifact(dataset.name, INDEX_FILE)
    if path.is_file():
        with open(path, "r") as fh:
            meta.update(json.load(fh))
    else:
        log.error("No index file found", dataset=dataset.name, report_issue=False)
    meta.update(dataset.to_opensanctions_dict(catalog))
    if len(meta["resources"]) == 0:
        log.info("Dataset has no resources", dataset=dataset.name)
    return meta


def get_catalog_datasets(scope: Dataset) -> List[Dict[str, Any]]:
    datasets = []
    for dataset in scope.datasets:
        datasets.append(get_catalog_dataset(dataset))
    return datasets


def write_issues(dataset: Dataset, max_export: int = 1_000) -> None:
    """Export list of data issues from crawl stage."""
    if dataset.is_collection:
        return
    issues = DatasetIssues(dataset)
    export_issues: List[Issue] = []
    for issue in issues.all():
        if len(export_issues) >= max_export:
            log.warning(
                "Maximum issue count for export exceeded, check the issue log instead.",
                max_export=max_export,
            )
            break
        export_issues.append(issue)
    issues_path = dataset_resource_path(dataset.name, ISSUES_FILE)
    log.info("Writing dataset issues list...", path=issues_path.as_posix())
    with open(issues_path, "wb") as fh:
        data = {"issues": export_issues}
        write_json(data, fh)


def write_delta_index(
    dataset: Dataset, max_versions: int = 100, include_latest: bool = True
) -> None:
    """Export list of delta data versions for the dataset with their URLs
    associated."""
    versions: Dict[str, str] = {}

    # This hasn't been uploaded yet, but will become available at the same
    # time as the index file:
    latest = get_latest(dataset.name, backfill=False)
    if latest is not None and include_latest:
        data_path = dataset_resource_path(dataset.name, DELTA_EXPORT_FILE)
        if data_path.is_file() and data_path.stat().st_size > 0:
            versions[latest.id] = make_artifact_url(
                dataset.name, latest.id, DELTA_EXPORT_FILE
            )

    # Get the most recent versions of the dataset:
    for version in iter_dataset_versions(dataset.name):
        if version.id in versions:
            continue
        object = get_artifact_object(
            dataset.name, DELTA_EXPORT_FILE, version=version.id
        )
        if object is not None and object.size() > 0:
            versions[version.id] = make_artifact_url(
                dataset.name, version.id, DELTA_EXPORT_FILE
            )
        if len(versions) >= max_versions:
            break

    # A hopefully temporary stand-in for tooling that doesn't support iterating
    # over object keys https://github.com/opensanctions/opensanctions/issues/2216
    version_list = [
        {
            "version": version_id,
            "url": version_url,
        }
        for version_id, version_url in versions.items()
    ]

    if len(versions) == 0:
        log.info("No delta versions found", dataset=dataset.name)
        return
    index_path = dataset_resource_path(dataset.name, DELTA_INDEX_FILE)
    log.info("Writing delta versions index...", path=index_path.as_posix())
    with open(index_path, "wb") as fh:
        data = {
            "versions": versions,
            "unstable": {"version_list": version_list},
        }

        write_json(data, fh)


def write_catalog(scope: Dataset) -> None:
    """Export a Nomenklatura-style data catalog file to represent all the datasets
    within this scope."""
    if not scope.is_collection:
        return
    catalog_path = dataset_resource_path(scope.name, CATALOG_FILE)
    log.info("Writing collection as catalog...", path=catalog_path.as_posix())
    with open(catalog_path, "wb") as fh:
        data = {
            "datasets": get_catalog_datasets(scope),
            "updated_at": settings.RUN_TIME_ISO,
        }
        write_json(data, fh)
