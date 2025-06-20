import logging
import re
import sys
import yaml

from datetime import datetime, timedelta
from pathlib import Path
from typing import List

"""
This script automates the maintenance of dataset metadata by:

1. Scanning all dataset YAML files (*.yml) under the 'datasets/' directory.
2. For each dataset, checking the 'manual_check' section to determine if a manual review is due.
   - The review is considered due if 'last_checked' plus 'interval' days is before or equal to today.
3. If due:
   - A message about the dataset is collected for reporting.
   - The 'last_checked' field is updated to today's date.
   - The updated YAML is saved, preserving its formatting.

It outputs a Markdown-formatted summary of datasets due for manual review.
"""

log = logging.getLogger(__name__)
# Strictly match the line: `  last_checked: "YYYY-MM-DD"`
LAST_CHECKED = r'^(?P<indent>\s*)last_checked:\s*"\d{4}-\d{2}-\d{2}"\s*$'


def load_yaml_file(yml_path: Path):
    with yml_path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def is_due_for_manual_check(manual_check: dict) -> bool:
    """Check if manual check is due."""
    last_checked_str = manual_check.get("last_checked")
    interval_days = manual_check.get("interval")

    if not last_checked_str:
        return False  # No last check date — skip

    try:
        last_checked = datetime.strptime(last_checked_str, "%Y-%m-%d")
    except ValueError:
        log.warning("Couldn't parse last_checked %s" % (last_checked_str))
        return True  # Malformed date, better to check

    return datetime.today() >= (last_checked + timedelta(days=interval_days))


def patch_last_checked_line(path: Path):
    today = datetime.today().strftime("%Y-%m-%d")
    replacement = r'\g<indent>last_checked: "{}"'.format(today)
    with path.open("r", encoding="utf-8") as f:
        content = f.read()
    # Replace only the last_checked line — no multiline overreach
    new_content = re.sub(LAST_CHECKED, replacement, content, flags=re.MULTILINE)

    with path.open("w", encoding="utf-8") as f:
        f.write(new_content)


def process_datasets(root: Path) -> List[tuple]:
    """Process datasets and return names that are due for manual review."""
    due = []
    for yml_path in root.rglob("*.yml"):
        data = load_yaml_file(yml_path)
        dataset_name = data.get("name", yml_path.stem)

        manual_check = data.get("manual_check")
        if not manual_check:
            continue

        if is_due_for_manual_check(manual_check):
            message = manual_check.get("message", "Dataset due for manual review.")
            due.append((dataset_name, message))

        patch_last_checked_line(yml_path)

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
    # this is intended for github actions which considers nonzero an error condition,
    # marking the job as failed.
    # So we use no output to mean nothing to be done (no github issue needs to be created)
    else:
        sys.exit(0)
