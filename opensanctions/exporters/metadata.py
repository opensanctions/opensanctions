from typing import Any, Dict
from functools import cache
from urllib.parse import urljoin
from followthemoney import model
from nomenklatura.matching import explain_matcher

from opensanctions import settings
from opensanctions.core.logs import get_logger
from opensanctions.core.db import engine_read
from opensanctions.core.dataset import Dataset
from opensanctions.core.issues import all_issues, agg_issues_by_level
from opensanctions.core.resources import all_resources
from opensanctions.core.statements import (
    all_schemata,
    max_last_seen,
    count_entities,
    agg_entities_by_country,
    agg_entities_by_schema,
)
from opensanctions.exporters.common import write_json

log = get_logger(__name__)
THINGS = [s.name for s in model if s.is_a("Thing")]


@cache
def dataset_to_index(dataset: Dataset) -> Dict[str, Any]:
    with engine_read() as conn:
        meta = dataset.to_dict()
        meta["last_change"] = max_last_seen(conn, dataset)
        meta["last_export"] = settings.RUN_TIME
        meta["index_url"] = dataset.make_public_url("index.json")
        meta["issues_url"] = dataset.make_public_url("issues.json")
        meta["issue_levels"] = agg_issues_by_level(conn, dataset)
        meta["issue_count"] = sum(meta["issue_levels"].values())
        meta["entity_count"] = count_entities(conn, dataset=dataset)
        target_count = count_entities(conn, dataset=dataset, target=True)
        meta["target_count"] = target_count
        meta["targets"] = {
            "total": target_count,
            "countries": agg_entities_by_country(conn, dataset, target=True),
            "schemata": agg_entities_by_schema(conn, dataset, target=True),
        }
        meta["things"] = {
            "total": count_entities(conn, dataset=dataset, schemata=THINGS),
            "countries": agg_entities_by_country(conn, dataset, schemata=THINGS),
            "schemata": agg_entities_by_schema(conn, dataset, schemata=THINGS),
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
            "statements_url": urljoin(settings.DATASET_URL, "statements.csv"),
            "model": model,
            "schemata": schemata,
            "matcher": explain_matcher(),
            "app": "opensanctions",
            "version": settings.VERSION,
        }
        write_json(meta, fh)
