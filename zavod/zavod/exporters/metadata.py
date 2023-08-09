import json
from typing import Any, Dict, cast

from zavod import settings
from zavod.logs import get_logger
from zavod.meta import Dataset
from zavod.archive import INDEX_FILE, STATISTICS_FILE, ISSUES_FILE
from zavod.archive import get_dataset_resource, dataset_resource_path
from zavod.runtime.resources import DatasetResources
from zavod.runtime.issues import DatasetIssues
from zavod.util import write_json

log = get_logger(__name__)


def get_dataset_statistics(dataset: Dataset) -> Dict[str, Any]:
    statistics_path = get_dataset_resource(dataset, STATISTICS_FILE)
    if not statistics_path.is_file():
        log.error("No statistics file found", dataset=dataset.name)
        return {}
    with open(statistics_path, "r") as fh:
        return cast(Dict[str, Any], json.load(fh))


def write_dataset_index(dataset: Dataset) -> None:
    """Export dataset metadata to index.json."""
    index_path = dataset_resource_path(dataset.name, INDEX_FILE)
    log.info(
        "Writing dataset index",
        path=index_path,
        is_collection=dataset.is_collection,
    )
    meta = dataset.to_opensanctions_dict()
    meta.update(get_dataset_statistics(dataset))
    if not dataset.is_collection:
        issues = DatasetIssues(dataset)
        meta["issue_levels"] = issues.by_level()
        meta["issue_count"] = sum(meta["issue_levels"].values())
    resources = DatasetResources(dataset)
    meta["resources"] = [r.to_opensanctions_dict() for r in resources.all()]
    meta["last_export"] = settings.RUN_TIME
    meta["index_url"] = dataset.make_public_url("index.json")
    meta["issues_url"] = dataset.make_public_url("issues.json")
    with open(index_path, "wb") as fh:
        write_json(meta, fh)


def write_issues(dataset: Dataset) -> None:
    """Export list of data issues from crawl stage."""
    if dataset.is_collection:
        return
    issues_path = dataset_resource_path(dataset.name, ISSUES_FILE)
    log.info("Writing dataset issues list", path=issues_path.as_posix())
    with open(issues_path, "wb") as fh:
        issues = DatasetIssues(dataset)
        data = {"issues": list(issues.all())}
        write_json(data, fh)
