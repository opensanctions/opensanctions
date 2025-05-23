import sys
from pathlib import Path
from typing import List
from datetime import datetime

from zavod.meta.catalog import ArchiveBackedCatalog
from zavod.meta.dataset import Dataset


def load_all_datasets(root: Path, catalog: ArchiveBackedCatalog) -> List[Dataset]:
    datasets = []
    for yml_path in root.rglob("*.yml"):
        dataset = catalog.load_yaml(yml_path)
        if dataset:
            datasets.append(dataset)
    return datasets


def has_frequency_never(dataset: Dataset) -> bool:
    coverage = dataset._data.get("coverage", {})
    frequency = coverage.get("frequency")
    return frequency == "never"


def is_due_for_check(dataset: Dataset, current_month: int) -> bool:
    manual_updates = dataset._data.get("manual_updates")
    if not manual_updates:
        return False
    try:
        manual_updates = int(manual_updates)
    except (TypeError, ValueError):
        return False

    return (current_month % manual_updates) == 0


def get_due_datasets(datasets: List[Dataset], current_month: int) -> List[str]:
    due = []
    for dataset in datasets:
        if has_frequency_never(dataset) and is_due_for_check(dataset, current_month):
            due.append(dataset.name)
    return due


if __name__ == "__main__":
    catalog = ArchiveBackedCatalog()
    datasets = load_all_datasets(Path("datasets/"), catalog)
    now = datetime.now()
    current_month = now.month

    due = get_due_datasets(datasets, current_month)

    print("::set-output name=due_datasets::" + ", ".join(due))

    if due:
        print("## Datasets due for manual review\n")
        for name in due:
            print(f"- {name}")
        sys.exit(0)
    else:
        print("No datasets are currently due for manual review.")
        sys.exit(1)
