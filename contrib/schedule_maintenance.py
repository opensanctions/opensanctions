import sys
import ruamel.yaml

from pathlib import Path
from typing import List
from datetime import datetime, timedelta

"""
This script automates the maintenance of dataset metadata by:

1. Scanning all dataset YAML files (*.yml and *.yaml) under the 'datasets/' directory.
2. For each dataset, checking the 'manual_check' section to determine if a manual review is due.
   - The review is considered due if 'last_checked' plus 'interval' days is before or equal to today.
3. If due:
   - A message about the dataset is collected for reporting.
   - The 'last_checked' field is updated to today's date.
   - The updated YAML is saved, preserving its formatting.

It outputs a Markdown-formatted summary of datasets due for manual review.
"""

yaml = ruamel.yaml.YAML(typ="rt")
yaml.width = 10000
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.preserve_quotes = True
yaml.default_flow_style = False


def load_yaml_file(yml_path: Path):
    """Load YAML file using ruamel.yaml."""
    with yml_path.open() as f:
        return yaml.load(f)


def save_yaml_file(data, yml_path: Path):
    """Save YAML file using ruamel.yaml."""
    with yml_path.open("w") as f:
        yaml.dump(data, f)


def is_due_for_manual_check(manual_check: dict) -> bool:
    """Check if manual check is due."""
    last_checked_str = manual_check.get("last_checked")
    interval_days = manual_check.get("interval", 90)

    if not last_checked_str:
        return False  # No last check date — skip

    try:
        last_checked = datetime.strptime(last_checked_str, "%Y-%m-%d")
    except ValueError:
        return True  # Malformed date, better to check

    return datetime.today() >= (last_checked + timedelta(days=interval_days))


def update_last_checked(manual_check: dict):
    """Update 'last_checked' to today."""
    manual_check["last_checked"] = datetime.today().strftime("%Y-%m-%d")


def process_datasets(root: Path) -> List[tuple]:
    """Process datasets and return names that are due for manual review."""
    due = []
    # TODO: Remove this once all extensions are migrated to .yml
    for ext in ("*.yml", "*.yaml"):
        for yml_path in root.rglob(ext):
            data = load_yaml_file(yml_path)
            dataset_name = data.get("name", yml_path.stem)

            manual_check = data.get("manual_check")
            if not manual_check:
                continue

            if is_due_for_manual_check(manual_check):
                message = manual_check.get("message", "Dataset due for manual review.")
                due.append((dataset_name, message))

                update_last_checked(manual_check)
                save_yaml_file(data, yml_path)

    return due


if __name__ == "__main__":
    datasets_root = Path("datasets/")
    due_datasets = process_datasets(datasets_root)

    if due_datasets:
        print("## Datasets due for manual review\n")
        for dataset_name, message in due_datasets:
            link = f"https://www.opensanctions.org/datasets/{dataset_name}/"
            print(f"- [ ] [{dataset_name}]({link}): {message}")
        sys.exit(0)
    # Although a lot of programs use nonzero exit for some status other than an error,
    # in github actions, it's usually considered an error condition and the job will be
    # marked failed and it'll email the last committer to the action. That's why we use
    # truthiness of the otuput to determine if the job was successful or not.
    else:
        sys.exit(0)
