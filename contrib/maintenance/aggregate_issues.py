"""Aggregate every issue of a dataset's latest run into message patterns.

    python -m contrib.maintenance.aggregate_issues <dataset_name>

The diagnose report inlines small issue sets and caps its grouped view; this
is the full-depth companion for noisy datasets: it streams the entire
issues.log of the latest run — no cap — and prints every message pattern with
its count and one example. A maintainer scratch tool; its output format is
unstable.
"""

import argparse
import json
import sys

from .archive import get_versions, iter_issues
from .issues import group_issues


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Aggregate all issues of a dataset's latest run by message pattern."
    )
    parser.add_argument("name", help="dataset name, e.g. gb_coh_psc")
    args = parser.parse_args()

    versions = get_versions(args.name)
    if versions is None or versions.latest is None:
        print(f"No archived runs found for: {args.name}", file=sys.stderr)
        sys.exit(1)
    try:
        issues = list(iter_issues(args.name, versions.latest))
    except FileNotFoundError:
        print(f"Run {versions.latest} has no issues.log.", file=sys.stderr)
        sys.exit(1)

    groups = group_issues(issues)
    run_state = "failed" if versions.latest_failed else "successful"
    print(
        f"{len(issues)} issues in {run_state} run {versions.latest} "
        f"of {args.name}; {len(groups)} message patterns:\n"
    )
    for _, members in groups:
        example = members[0]
        print(f"[{len(members)}] {example.get('level')}: {example.get('message')}")
        data = example.get("data")
        if isinstance(data, dict):
            data = {k: v for k, v in data.items() if k != "dataset"}
        if data:
            payload = json.dumps(data, ensure_ascii=False)
            if len(payload) > 200:
                payload = payload[:200] + "…"
            print(f"    e.g. {payload}")


if __name__ == "__main__":
    main()
