import click
import logging
from pathlib import Path
from typing import Set

from zavod.logs import get_logger, configure_logging
from zavod.meta import Dataset, get_catalog

log = get_logger("check_hierarchy")

InDir = click.Path(dir_okay=True, readable=True, file_okay=False, path_type=Path)


@click.command()
@click.argument("datasets_path", type=InDir)
def main(datasets_path: Path):
    configure_logging(level=logging.INFO)
    catalog = get_catalog()
    for path in datasets_path.glob("**/*.y*ml"):
        catalog.load_yaml(path)

    collections: Set[Dataset] = set()
    children: Set[Dataset] = set()
    for dataset in catalog.datasets:
        if not len(dataset.model.children) and dataset.model.entry_point is None:
            log.warn(
                f"Dataset {dataset.name!r} has neither children nor an entry_point for crawling"
            )
            continue
        if dataset.is_collection:
            collections.add(dataset)
            children.update(dataset.children)

    for dataset in catalog.datasets:
        if dataset.is_collection:
            continue
        if dataset.model.disabled:
            continue
        if dataset not in children:
            log.warn(f"Dataset {dataset.name!r} has no collections")


if __name__ == "__main__":
    main()
