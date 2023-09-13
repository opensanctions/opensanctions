import json
from typing import List, Dict, Any
from urllib.parse import urljoin
from followthemoney import model

from zavod import settings
from zavod.archive import get_dataset_resource, datasets_path
from zavod.util import write_json
from zavod.meta import Dataset
from zavod.archive import INDEX_FILE
from zavod.logs import get_logger

log = get_logger(__name__)
SCOPED = ["datasets", "scopes", "sources", "externals", "collections"]


def catalog_datasets(scope: Dataset) -> List[Dict[str, Any]]:
    datasets = []
    for dataset in scope.datasets:
        path = get_dataset_resource(dataset, INDEX_FILE)
        if not path.is_file():
            log.error("No index file found", dataset=dataset.name, report_issue=False)
            continue

        with open(path, "r") as fh:
            metadata: Dict[str, Any] = json.load(fh)

            # Make sure the datasets in this catalog don't reference datasets
            # that are not included in the scope.
            for field in SCOPED:
                if field in metadata:
                    values = metadata.get(field, [])
                    values = [d for d in values if d in scope.dataset_names]
                    metadata[field] = values

            datasets.append(metadata)
    return datasets


def get_opensanctions_catalog(scope: Dataset) -> Dict[str, Any]:
    """Get the OpenSanctions-style catalog, including all datasets in the given
    scope."""
    datasets = catalog_datasets(scope)
    schemata = set()
    for ds in datasets:
        schemata.update(ds.get("schemata", []))

    stmt_url = urljoin(settings.DATASET_URL, "latest/default/statements.csv")
    log.info("Generating catalog", schemata=len(schemata), datasets=len(datasets))
    return {
        "datasets": datasets,
        "run_time": settings.RUN_TIME,
        "dataset_url": settings.DATASET_URL,
        "statements_url": stmt_url,
        "model": model.to_dict(),
        "schemata": sorted(schemata),
        "app": "opensanctions",
    }


def get_nk_catalog(scope: Dataset) -> Dict[str, Any]:
    """Get the Nomenklatura-style catalog, including all datasets in the given
    scope."""
    datasets = catalog_datasets(scope)
    return {"datasets": datasets}


def export_index(scope: Dataset) -> None:
    """Export the global index for all datasets in the given scope."""
    base_path = datasets_path()
    meta = get_opensanctions_catalog(scope)
    index_path = base_path.joinpath(INDEX_FILE)
    log.info("Writing global index", datasets=len(meta["datasets"]), path=index_path)
    with open(index_path, "wb") as fh:
        write_json(meta, fh)
