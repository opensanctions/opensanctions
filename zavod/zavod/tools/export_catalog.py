import json
from typing import Dict, Any, Optional
from followthemoney import model, registry
from followthemoney.dataset import Version

from zavod import settings
from zavod.util import write_json
from zavod.meta import Dataset
from zavod.runtime.urls import make_artifact_url, make_published_url
from zavod.exporters.metadata import get_catalog_datasets
from zavod.archive import datasets_path, get_dataset_artifact
from zavod.archive import find_archive_artifact, latest_local_version
from zavod.archive import INDEX_FILE, STATISTICS_FILE
from zavod.logs import get_logger

log = get_logger(__name__)


def _latest_version(dataset_name: str, resource: str) -> Optional[Version]:
    """Resolve the newest run of a dataset that has the given resource: a local run
    if present, otherwise the newest one available in the archive."""
    version = latest_local_version(dataset_name, with_resource=resource)
    if version is None:
        version, _ = find_archive_artifact(dataset_name, resource)
    return version


def get_opensanctions_catalog(scope: Dataset) -> Dict[str, Any]:
    """Get the OpenSanctions-style catalog, including all datasets in the given
    scope."""
    datasets = get_catalog_datasets(scope)

    schemata = set()
    stats_version = _latest_version(scope.name, STATISTICS_FILE)
    if stats_version is not None:
        statistics_path = get_dataset_artifact(
            scope.name, stats_version, STATISTICS_FILE
        )
        if statistics_path.is_file():
            with open(statistics_path, "r") as fh:
                stats: Dict[str, Any] = json.load(fh)
                schemata.update(stats.get("schemata", []))

    log.info("Generating catalog", schemata=len(schemata), datasets=len(datasets))
    default_version = _latest_version("default", "statements.csv")
    if default_version is not None:
        statements_url = make_artifact_url(
            "default", default_version.id, "statements.csv"
        )
    else:
        # TODO: Remove after default dataset is published.
        # https://github.com/opensanctions/operations/issues/2587
        statements_url = make_published_url("default", "statements.csv")
    return {
        "datasets": datasets,
        "run_time": settings.RUN_TIME_ISO,
        "statements_url": statements_url,
        "model": model.to_dict(),
        "target_topics": registry.topic.RISKS,
        "enrich_topics": settings.ENRICH_TOPICS,
        "schemata": sorted(schemata),
        "app": "opensanctions",
    }


def get_nk_catalog(scope: Dataset) -> Dict[str, Any]:
    """Get the Nomenklatura-style catalog, including all datasets in the given
    scope."""
    datasets = get_catalog_datasets(scope)
    return {"datasets": datasets, "updated_at": settings.RUN_TIME_ISO}


def export_index(scope: Dataset) -> None:
    """Export the global index for all datasets in the given scope."""
    base_path = datasets_path()
    meta = get_opensanctions_catalog(scope)
    index_path = base_path.joinpath(INDEX_FILE)
    log.info("Writing global index", datasets=len(meta["datasets"]), path=index_path)
    with open(index_path, "wb") as fh:
        write_json(meta, fh)
