import json
from typing import Any, Dict
from urllib.parse import urljoin
from followthemoney import model
from nomenklatura.matching import MatcherV1

from zavod import settings
from zavod.logs import get_logger
from zavod.meta import Dataset
from zavod.archive import get_dataset_resource, datasets_path
from zavod.archive import INDEX_FILE
from opensanctions.core.db import engine_tx
from opensanctions.core.issues import all_issues, agg_issues_by_level
from opensanctions.core.resources import all_resources
from opensanctions.util import write_json

log = get_logger(__name__)
THINGS = [s.name for s in model if s.is_a("Thing")]


def get_dataset_statistics(dataset: Dataset) -> Dict[str, Any]:
    statistics_path = get_dataset_resource(dataset, "statistics.json")
    if not statistics_path.exists():
        log.error("No statistics file found", dataset=dataset.name)
        return {}
    with open(statistics_path, "r") as fh:
        return json.load(fh)


def dataset_to_index(dataset: Dataset) -> Dict[str, Any]:
    meta = dataset.to_opensanctions_dict()
    meta.update(get_dataset_statistics(dataset))
    meta["last_export"] = settings.RUN_TIME
    meta["index_url"] = dataset.make_public_url("index.json")
    meta["issues_url"] = dataset.make_public_url("issues.json")
    meta["issue_levels"] = agg_issues_by_level(dataset)
    with engine_tx() as conn:
        meta["resources"] = list(all_resources(conn, dataset))
    meta["issue_count"] = sum(meta["issue_levels"].values())
    return meta


def export_metadata(scope: Dataset) -> None:
    """Export the global index for all datasets."""
    base_path = datasets_path()
    datasets = []
    schemata = set()
    for dataset in scope.datasets:
        ds_path = get_dataset_resource(dataset, INDEX_FILE)
        if ds_path is None or not ds_path.exists():
            log.error("No index file found", dataset=dataset.name, report_issue=False)
        else:
            with open(ds_path, "r") as fh:
                ds_data = json.load(fh)
                schemata.update(ds_data.get("schemata", []))
                datasets.append(ds_data)

    issues_path = base_path.joinpath("issues.json")
    log.info("Writing global issues list", path=issues_path)
    with open(issues_path, "wb") as fh:
        issues = all_issues(scope)
        data = {"issues": list(issues)}
        write_json(data, fh)

    index_path = base_path.joinpath(INDEX_FILE)
    log.info("Writing global index", datasets=len(datasets), path=index_path)
    with open(index_path, "wb") as fh:
        meta = {
            "datasets": datasets,
            "run_time": settings.RUN_TIME,
            "dataset_url": settings.DATASET_URL,
            "issues_url": urljoin(settings.DATASET_URL, "issues.json"),
            "statements_url": urljoin(settings.DATASET_URL, "statements.csv"),
            "model": model.to_dict(),
            "schemata": list(schemata),
            "matcher": MatcherV1.explain(),
            "app": "opensanctions",
        }
        write_json(meta, fh)
