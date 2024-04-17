from collections import defaultdict
from typing import Dict, List
from glob import glob
from pathlib import Path

from zavod.meta.dataset import Dataset
from zavod.meta import get_catalog


ROOTS = {
    "graph",
    "all",
    "securities",
    "sanctions_unresolved",
    "work",
    "incoming",
    "openownership",
    "alert_testing",
}

catalog = get_catalog()
yamls = glob("datasets/**/*.y*ml", recursive=True)
for yaml in yamls:
    catalog.load_yaml(Path(yaml))

dataset_collections: Dict[str, List[str]] = defaultdict(list)

# For each dataset with children, do all of its children exist?
for dataset in catalog.datasets:
    # nomenklatura.dataset.Dataset.children will log missing datasets on access.
    for child in dataset.children:
        dataset_collections[child.name].append(dataset.name)

# Is each dataset in at least one collection?
for dataset in catalog.datasets:
    if dataset.name not in ROOTS and not dataset_collections[dataset.name]:
        print(f"Dataset {dataset} is not in any collections")
