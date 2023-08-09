from followthemoney import model
from zavod.archive import get_dataset_resource, datasets_path
from zavod.util import write_json
from zavod.meta import Dataset
from zavod.archive import INDEX_FILE
from zavod.logs import get_logger
from zavod.runtime.issues import DatasetIssues
from zavod import settings
from urllib.parse import urljoin
from nomenklatura.matching import MatcherV1
import json

log = get_logger(__name__)
SCOPED = ["datasets", "scopes", "sources", "externals", "collections"]


def export_index(scope: Dataset) -> None:
    """Export the global index for all datasets in the given scope."""
    base_path = datasets_path()
    datasets = []
    schemata = set()
    for dataset in scope.datasets:
        ds_path = get_dataset_resource(dataset, INDEX_FILE)
        if not ds_path.is_file():
            log.error("No index file found", dataset=dataset.name, report_issue=False)
        else:
            with open(ds_path, "r") as fh:
                ds_data = json.load(fh)

                # Make sure the datasets in this catalog don't reference datasets
                # that are not included in the scope.
                for field in SCOPED:
                    if field in ds_data:
                        values = ds_data.get(field, [])
                        values = [d for d in values if d in scope.dataset_names]
                        ds_data[field] = values

                schemata.update(ds_data.get("schemata", []))
                datasets.append(ds_data)

    issues_path = base_path.joinpath("issues.json")
    log.info("Writing global issues list", path=issues_path)
    with open(issues_path, "wb") as fh:
        issues = DatasetIssues(scope)
        data = {"issues": list(issues.all())}
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
