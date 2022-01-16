import structlog
from typing import Any, Dict
from functools import cache
from urllib.parse import urljoin
from followthemoney import model

from opensanctions import settings
from opensanctions.core.db import engine_read
from opensanctions.core.dataset import Dataset
from opensanctions.core.issues import all_issues, agg_issues_by_level
from opensanctions.core.resources import all_resources
from opensanctions.core.statements import all_schemata, max_last_seen, count_entities
from opensanctions.core.statements import agg_targets_by_country, agg_targets_by_schema
from opensanctions.exporters.common import write_json

log = structlog.get_logger(__name__)


@cache
def dataset_to_index(dataset: Dataset) -> Dict[str, Any]:
    with engine_read() as conn:
        issue_levels = agg_issues_by_level(conn, dataset)
        target_count = count_entities(conn, dataset=dataset, target=True)
        last_change = max_last_seen(conn, dataset)
        target_countries = agg_targets_by_country(conn, dataset)
        target_schemata = agg_targets_by_schema(conn, dataset)
        meta = dataset.to_dict()
        meta["index_url"] = dataset.make_public_url("index.json")
        meta["issues_url"] = dataset.make_public_url("issues.json")
        meta["statements_url"] = dataset.make_public_url("statements.csv")
        meta["issue_levels"] = issue_levels
        meta["issue_count"] = sum(meta["issue_levels"].values())
        meta["target_count"] = target_count
        meta["last_change"] = last_change
        meta["last_export"] = settings.RUN_TIME
        meta["targets"] = {
            "countries": target_countries,
            "schemata": target_schemata,
        }
        meta["resources"] = list(all_resources(conn, dataset))
        return meta


def export_metadata():
    """Export the global index for all datasets."""
    datasets = []
    for dataset in Dataset.all():
        datasets.append(dataset_to_index(dataset))

    with engine_read() as conn:
        issues = list(all_issues(conn))
        schemata = all_schemata(conn)

    issues_path = settings.DATASET_PATH.joinpath("issues.json")
    log.info("Writing global issues list", path=issues_path)
    with open(issues_path, "w", encoding=settings.ENCODING) as fh:
        data = {"issues": issues}
        write_json(data, fh)

    index_path = settings.DATASET_PATH.joinpath("index.json")
    log.info("Writing global index", datasets=len(datasets), path=index_path)
    with open(index_path, "w", encoding=settings.ENCODING) as fh:
        meta = {
            "datasets": datasets,
            "run_time": settings.RUN_TIME,
            "dataset_url": settings.DATASET_URL,
            "issues_url": urljoin(settings.DATASET_URL, "issues.json"),
            "model": model,
            "schemata": schemata,
            "app": "opensanctions",
            "version": settings.VERSION,
        }
        write_json(meta, fh)
