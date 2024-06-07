import json
from typing import Any, Dict, List

from zavod import settings
from zavod.logs import get_logger
from zavod.meta import Dataset
from zavod.archive import INDEX_FILE, STATISTICS_FILE, ISSUES_FILE, CATALOG_FILE
from zavod.archive import get_dataset_artifact, dataset_resource_path
from zavod.runtime.urls import make_published_url, make_artifact_url
from zavod.runtime.resources import DatasetResources
from zavod.runtime.issues import DatasetIssues, Issue
from zavod.runtime.versions import get_latest
from zavod.util import write_json

log = get_logger(__name__)


def get_base_dataset_metadata(dataset: Dataset) -> Dict[str, Any]:
    meta = {
        "issue_levels": {},
        "issue_count": 0,
        "updated_at": settings.RUN_TIME_ISO,
        "index_url": make_published_url(dataset.name, "index.json"),
    }

    # This reads the file produced by the statistics exporter which
    # contains entity counts for the dataset, aggregated by various
    # criteria:
    statistics_path = get_dataset_artifact(dataset.name, STATISTICS_FILE)
    if statistics_path.is_file():
        with open(statistics_path, "r") as fh:
            stats: Dict[str, Any] = json.load(fh)
            meta.update(stats)

    resources = DatasetResources(dataset)
    meta["resources"] = [r.to_opensanctions_dict() for r in resources.all()]
    return meta


def write_dataset_index(dataset: Dataset) -> None:
    """Export dataset metadata to index.json."""
    version = get_latest(dataset.name, backfill=True)
    if version is None:
        raise ValueError(f"No version found for dataset: {dataset.name}")
    index_path = dataset_resource_path(dataset.name, INDEX_FILE)
    log.info(
        "Writing dataset index",
        path=index_path,
        is_collection=dataset.is_collection,
    )
    meta = get_base_dataset_metadata(dataset)
    meta.update(dataset.to_opensanctions_dict())
    if not dataset.is_collection:
        issues = DatasetIssues(dataset)
        meta["issue_levels"] = issues.by_level()
        meta["issue_count"] = sum(meta["issue_levels"].values())
    meta["last_export"] = settings.RUN_TIME_ISO
    meta["version"] = version.id.lower()
    meta["issues_url"] = make_artifact_url(dataset.name, version.id, "issues.json")
    with open(index_path, "wb") as fh:
        write_json(meta, fh)


def get_catalog_dataset(dataset: Dataset) -> Dict[str, Any]:
    """Get a metadata description of a single dataset, retaining timestamp information
    for the last export, but updating some other metadata."""
    meta = get_base_dataset_metadata(dataset)
    path = get_dataset_artifact(dataset.name, INDEX_FILE)
    if path.is_file():
        with open(path, "r") as fh:
            meta.update(json.load(fh))
    else:
        log.error("No index file found", dataset=dataset.name, report_issue=False)
    meta.update(dataset.to_opensanctions_dict())
    if len(meta["resources"]) == 0:
        log.warn("Dataset has no resources", dataset=dataset.name)
    # assert len(meta["resources"]), (dataset, meta["resources"])
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
