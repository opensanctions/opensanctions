"""Print the crawler source file belonging to each given dataset YAML.

    python -m contrib.maintenance.crawler_code <dataset.yml> [<dataset.yml> ...]

Use this from shell tooling that needs a dataset's code, not just its metadata —
contrib/lint_dataset.sh, for instance, takes a YAML path and has to find the
crawler to lint. Many datasets have no dataset-local code at all: the ~28
enrichment datasets point their entry_point at an installed module such as
`zavod.runner.local_enricher:enrich`. Those simply produce no output line, so a
caller reading stdout gets an empty list rather than an error.

Prints one resolved path per line, in argument order, omitting datasets without
local code. Exits non-zero only if a YAML cannot be read.
"""

import argparse
import sys

from .datasets import get_code_path, read_dataset_meta


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print the crawler source file for each dataset YAML, if it has one."
    )
    parser.add_argument(
        "yaml_paths",
        nargs="+",
        metavar="dataset.yml",
        help="path to a dataset metadata YAML, e.g. datasets/au/nsw_parliament/au_nsw_parliament.yml",
    )
    args = parser.parse_args()

    status = 0
    for yaml_path in args.yaml_paths:
        try:
            meta = read_dataset_meta(yaml_path)
        except (OSError, AssertionError) as exc:
            print(f"crawler_code: cannot read {yaml_path}: {exc}", file=sys.stderr)
            status = 1
            continue
        code_path = get_code_path(yaml_path, meta.get("entry_point"))
        if code_path is not None:
            print(code_path)
    sys.exit(status)


if __name__ == "__main__":
    main()
