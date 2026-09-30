"""Summarise a PEP crawl: occupancy status and date ranges per position.

Usage: python .claude/skills/crawler-pep/scripts/pep_summary.py <dataset_name>

Reads the statements of the latest `zavod crawl` run and prints, for each Position,
how many occupancies are current/ended/unknown and the span of their start and end
dates. Compare the `current` column with the number of seats the body actually has —
see the crawler-pep skill, Step 3.
"""

import csv
import sys
from collections import Counter, defaultdict
from pathlib import Path


def span(dates: list[str]) -> str:
    return f"{min(dates)[:10]}..{max(dates)[:10]}" if dates else "-"


def find_statements(dataset: str) -> Path:
    """The latest run's statements: per-run artifact folders (named by sortable run
    version), or the dataset folder itself in older zavod versions."""
    root = Path("data/datasets") / dataset
    runs = sorted(root.glob("_artifacts/*/statements.pack"))
    if len(runs) > 0:
        return runs[-1]
    path = root / "statements.pack"
    if not path.exists():
        sys.exit(f"No statements under {root} — run `zavod crawl` first")
    return path


def main(dataset: str) -> None:
    path = find_statements(dataset)
    print(f"Reading {path}")

    position_names: dict[str, str] = {}
    occupancies: dict[str, dict[str, list[str]]] = defaultdict(
        lambda: defaultdict(list)
    )
    persons: set[str] = set()
    with path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            schema, _, prop = row["prop"].partition(":")
            if schema == "Position" and prop == "name":
                position_names.setdefault(row["entity_id"], row["value"])
            elif schema == "Occupancy":
                occupancies[row["entity_id"]][prop].append(row["value"])
            elif schema == "Person":
                persons.add(row["entity_id"])

    status: dict[str, Counter[str]] = defaultdict(Counter)
    starts: dict[str, list[str]] = defaultdict(list)
    ends: dict[str, list[str]] = defaultdict(list)
    for props in occupancies.values():
        for post in props["post"]:
            # make_occupancy never writes an `unknown` status, so a missing one is it.
            status[post][props["status"][0] if props["status"] else "unknown"] += 1
            starts[post].extend(props["startDate"] + props["periodStart"])
            ends[post].extend(props["endDate"] + props["periodEnd"])

    print(f"Persons: {len(persons)}  Occupancies: {len(occupancies)}\n")
    header = ("current", "ended", "unknown", "start range", "end range", "position")
    print("{:>7} {:>7} {:>7}  {:<23}  {:<23}  {}".format(*header))
    for post, counts in sorted(status.items(), key=lambda x: -sum(x[1].values())):
        print(
            f"{counts['current']:>7} {counts['ended']:>7} {counts['unknown']:>7}  "
            f"{span(starts[post]):<23}  {span(ends[post]):<23}  "
            f"{position_names.get(post, post)}"
        )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
