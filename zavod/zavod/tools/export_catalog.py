from typing import Dict, Any
from followthemoney import model

from zavod import settings
from zavod.archive import datasets_path
from zavod.util import write_json
from zavod.meta import Dataset
from zavod.runtime.urls import make_published_url
from zavod.exporters.metadata import get_catalog_datasets
from zavod.archive import INDEX_FILE
from zavod.logs import get_logger

log = get_logger(__name__)


def get_opensanctions_catalog(scope: Dataset) -> Dict[str, Any]:
    """Get the OpenSanctions-style catalog, including all datasets in the given
    scope."""
    datasets = get_catalog_datasets(scope)
    schemata = set()
    for ds in datasets:
        schemata.update(ds.get("schemata", []))

    log.info("Generating catalog", schemata=len(schemata), datasets=len(datasets))
    return {
        "datasets": datasets,
        "run_version": str(settings.RUN_VERSION).lower(),
        "run_time": settings.RUN_TIME_ISO,
        "statements_url": make_published_url("default", "statements.csv"),
        "model": model.to_dict(),
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
