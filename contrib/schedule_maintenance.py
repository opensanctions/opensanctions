import sys
import os
import ruamel.yaml
from pathlib import Path
from typing import List, Tuple
from datetime import datetime, timedelta

from zavod.meta.catalog import ArchiveBackedCatalog
from zavod.meta.dataset import Dataset

yaml = ruamel.yaml.YAML(typ="rt")
yaml.width = 10000
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.preserve_quotes = True
yaml.default_flow_style = False


def load_dataset(yml_path: Path, catalog: ArchiveBackedCatalog) -> Tuple[Dataset, dict]:
    """Load Dataset object and YAML data."""
    dataset = catalog.load_yaml(yml_path)
    with yml_path.open() as f:
        data = yaml.load(f)
    return dataset, data


def has_frequency_never(dataset: Dataset) -> bool:
    """Check if dataset coverage frequency is 'never'."""
    return dataset._data.get("coverage", {}).get("frequency") == "never"


def is_due_for_manual_check(manual_check: dict) -> bool:
    last_checked_str = manual_check.get("last_checked")
    interval_days = manual_check.get("interval")
    if not last_checked_str:
        return False  # If last_checked is not set, do not check
    try:
        last_checked = datetime.strptime(last_checked_str, "%Y-%m-%d")
    except ValueError:
        return True  # Malformed date, better to check
    next_check = last_checked + timedelta(days=interval_days)
    return datetime.today() >= next_check


def process_datasets(root: Path, catalog: ArchiveBackedCatalog) -> List[str]:
    due = []
    for yml_path in root.rglob("*.yml"):
        dataset, data = load_dataset(yml_path, catalog)

        if not has_frequency_never(dataset):
            continue  # Only process datasets with frequency: never

        manual_check = data.get("manual_check")
        if manual_check and is_due_for_manual_check(manual_check):
            message = manual_check.get("message", "Dataset due for manual review.")
            print(f"- {dataset.name}: {message}")
            # Update last_checked
            manual_check["last_checked"] = datetime.today().strftime("%Y-%m-%d")
            # Write YAML back
            with yml_path.open("w") as f:
                yaml.dump(data, f)

            due.append(dataset.name)

    return due


if __name__ == "__main__":
    catalog = ArchiveBackedCatalog()
    datasets_root = Path("datasets/")
    due_datasets = process_datasets(datasets_root, catalog)

    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as output_file:
            output_file.write(f"due_datasets={','.join(due_datasets)}\n")

    if due_datasets:
        print("\n## Datasets due for manual review\n")
        for name in due_datasets:
            print(f"- {name}")
        sys.exit(0)
    else:
        print("No datasets are currently due for manual review.")
        sys.exit(1)
