#!/usr/bin/env python
from typing import Any

import click
import requests
from sqlalchemy import delete, func, select

from zavod.db import get_engine
from zavod.stateful.model import statement_table

CATALOG_URL = "https://data.opensanctions.org/datasets/latest/default/catalog.json"


def fetch_default_datasets() -> set[str]:
    """Fetch the dataset names included in the published default collection."""
    response = requests.get(CATALOG_URL, timeout=120)
    response.raise_for_status()
    catalog: dict[str, Any] = response.json()
    return {
        dataset["name"]
        for dataset in catalog.get("datasets", [])
        if isinstance(dataset.get("name"), str)
    }


def find_dead_datasets(default_datasets: set[str]) -> list[tuple[str, int]]:
    """Find stored statements belonging to datasets outside the default collection."""
    query = (
        select(statement_table.c.dataset, func.count())
        .where(statement_table.c.dataset.not_in(default_datasets))
        .group_by(statement_table.c.dataset)
        .order_by(func.count().desc())
    )
    with get_engine().connect() as conn:
        return [(dataset, count) for dataset, count in conn.execute(query)]


def delete_dataset(dataset: str) -> int:
    """Delete all statements belonging to one confirmed obsolete dataset."""
    query = delete(statement_table).where(statement_table.c.dataset == dataset)
    with get_engine().begin() as conn:
        result = conn.execute(query)
        return result.rowcount


@click.command()
def main() -> None:
    """Offer to delete statements absent from the published default collection."""
    default_datasets = fetch_default_datasets()
    dead_datasets = find_dead_datasets(default_datasets)
    if not dead_datasets:
        click.echo("No dead statement datasets found.")
        return

    click.echo(f"Found {len(dead_datasets)} dead statement datasets:")
    for dataset, count in dead_datasets:
        click.echo(f"{count:>12,}  {dataset}")

    for dataset, count in dead_datasets:
        if click.confirm(f"Delete {count:,} statements for {dataset!r}?"):
            deleted = delete_dataset(dataset)
            click.echo(f"Deleted {deleted:,} statements for {dataset!r}.")


if __name__ == "__main__":
    main()
