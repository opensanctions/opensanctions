from typing import Any, Dict
from urllib.parse import urljoin
from followthemoney import model
from zavod.logs import get_logger
from nomenklatura.matching import explain_matcher
from nomenklatura.cache import Cache
from nomenklatura.util import datetime_iso

from opensanctions import settings
from opensanctions.core.db import engine, metadata, engine_tx, engine_read
from opensanctions.core.dataset import Dataset
from opensanctions.core.issues import all_issues, agg_issues_by_level
from opensanctions.core.resources import all_resources
from opensanctions.core.statements import (
    all_schemata,
    max_last_seen,
    count_entities,
    last_modified,
    agg_entities_by_country,
    agg_entities_by_schema,
)
from opensanctions.util import write_json

log = get_logger(__name__)
THINGS = [s.name for s in model if s.is_a("Thing")]


def dataset_to_index(dataset: Dataset) -> Dict[str, Any]:
    log.info("Computing metadata stats...", dataset=dataset.name)
    cache = Cache(engine, metadata, dataset)
    with engine_tx() as conn:
        last_modified_date = last_modified(conn, dataset)
        stats_key = f"{dataset.name}:stats:{datetime_iso(last_modified_date)}"
        stats = cache.get_json(stats_key, max_age=7, conn=conn)
        if stats is None:
            target_count = count_entities(conn, dataset=dataset, target=True)
            stats = {
                "entity_count": count_entities(conn, dataset=dataset),
                "target_count": target_count,
                "targets": {
                    "total": target_count,
                    "countries": agg_entities_by_country(conn, dataset, target=True),
                    "schemata": agg_entities_by_schema(conn, dataset, target=True),
                },
                "things": {
                    "total": count_entities(conn, dataset=dataset, schemata=THINGS),
                    "countries": agg_entities_by_country(
                        conn, dataset, schemata=THINGS
                    ),
                    "schemata": agg_entities_by_schema(conn, dataset, schemata=THINGS),
                },
            }
            cache.set_json(stats_key, stats, conn=conn)

        meta = dataset.to_dict()
        meta.update(stats)
        meta["last_change"] = last_modified_date
        meta["last_export"] = settings.RUN_TIME
        meta["entity_count"] = count_entities(conn, dataset=dataset)
        meta["index_url"] = dataset.make_public_url("index.json")
        meta["issues_url"] = dataset.make_public_url("issues.json")
        meta["issue_levels"] = agg_issues_by_level(conn, dataset)
        meta["issue_count"] = sum(meta["issue_levels"].values())
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
    with open(issues_path, "wb") as fh:
        data = {"issues": issues}
        write_json(data, fh)

    index_path = settings.DATASET_PATH.joinpath("index.json")
    log.info("Writing global index", datasets=len(datasets), path=index_path)
    with open(index_path, "wb") as fh:
        meta = {
            "datasets": datasets,
            "run_time": settings.RUN_TIME,
            "dataset_url": settings.DATASET_URL,
            "issues_url": urljoin(settings.DATASET_URL, "issues.json"),
            "statements_url": urljoin(settings.DATASET_URL, "statements.csv"),
            "model": model.to_dict(),
            "schemata": schemata,
            "matcher": explain_matcher(),
            "app": "opensanctions",
            "version": settings.VERSION,
        }
        write_json(meta, fh)
