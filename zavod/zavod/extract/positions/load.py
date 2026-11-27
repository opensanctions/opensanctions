import os
import random
from collections import defaultdict
from datetime import datetime

import click
from rich.console import Console
from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Integer,
    MetaData,
    Table,
    Unicode,
    create_engine,
    select,
)

from models import DATA_DIR, Item, write_jsonl

console = Console()

# The subset of zavod.stateful.model.position_table that this script reads.
position_table = Table(
    "position",
    MetaData(),
    Column("id", Integer),
    Column("caption", Unicode),
    Column("countries", JSON),
    Column("subnational_areas", JSON),
    Column("dataset", Unicode),
    Column("created_at", DateTime),
    Column("last_seen", DateTime),
    Column("modified_by", Unicode),
    Column("deleted_at", DateTime),
)


def get_database_uri() -> str:
    # Same variables and precedence as zavod.settings.
    uri = os.environ.get("OPENSANCTIONS_DATABASE_URI") or os.environ.get(
        "ZAVOD_DATABASE_URI"
    )
    if uri is None:
        raise click.UsageError("Set ZAVOD_DATABASE_URI or OPENSANCTIONS_DATABASE_URI.")
    # SQLAlchemy defaults postgresql:// to psycopg2; this project ships psycopg 3.
    if uri.startswith("postgresql://"):
        uri = "postgresql+psycopg://" + uri.removeprefix("postgresql://")
    return uri


def sample_across_datasets(
    items: list[Item], size: int, rng: random.Random
) -> list[Item]:
    """Take items round-robin from shuffled datasets, so no single large dataset
    dominates the sample."""
    by_dataset: dict[str, list[Item]] = defaultdict(list)
    for item in items:
        by_dataset[item.dataset].append(item)
    queues = list(by_dataset.values())
    rng.shuffle(queues)
    for queue in queues:
        rng.shuffle(queue)

    sample: list[Item] = []
    while len(sample) < size:
        for queue in queues:
            if queue and len(sample) < size:
                sample.append(queue.pop())
    return sample


@click.command()
@click.option(
    "--sample",
    type=int,
    help="Size of the test subset. Defaults to 50, or to all items with --dataset.",
)
@click.option(
    "--random",
    "uniform",
    is_flag=True,
    help="Sample uniformly at random instead of evenly across datasets.",
)
@click.option(
    "--dataset",
    "datasets",
    multiple=True,
    help="Select the test subset from this dataset only. Repeatable.",
)
@click.option(
    "--modified-by",
    help="Only load positions last modified by this user, e.g. friedrich@opensanctions.org.",
)
@click.option(
    "--unreviewed",
    is_flag=True,
    help="Only load positions that no user has modified.",
)
@click.option(
    "--output",
    default="test",
    show_default=True,
    help="Name of the test subset file in data/, without .jsonl.",
)
@click.option("--skip-all", is_flag=True, help="Do not write data/all.jsonl.")
@click.option("--seed", default=0, show_default=True, help="Seed for the test subset.")
@click.option(
    "--since",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default="2026-07-01",
    show_default=True,
    help="Only load positions first seen after this date.",
)
def main(
    sample: int | None,
    uniform: bool,
    datasets: tuple[str, ...],
    modified_by: str | None,
    unreviewed: bool,
    output: str,
    skip_all: bool,
    seed: int,
    since: datetime,
) -> None:
    """Load all non-Wikidata position items with a last_seen date into data/all.jsonl and select a test subset from them."""
    table = position_table
    query = select(
        table.c.id,
        table.c.caption,
        table.c.countries,
        table.c.subnational_areas,
        table.c.dataset,
    ).where(
        table.c.deleted_at.is_(None),
        table.c.created_at > since,
        table.c.last_seen.is_not(None),
        ~table.c.dataset.like("wd\\_%", escape="\\"),
    )
    if modified_by is not None and unreviewed:
        raise click.UsageError("--modified-by and --unreviewed are mutually exclusive.")
    if modified_by is not None:
        query = query.where(table.c.modified_by == modified_by)
    if unreviewed:
        query = query.where(table.c.modified_by.is_(None))
    items: list[Item] = []
    engine = create_engine(get_database_uri())
    with console.status("Reading positions"), engine.connect() as conn:
        for row in conn.execute(query):
            items.append(
                Item(
                    id=row.id,
                    caption=row.caption,
                    countries=tuple(sorted(row.countries)),
                    subnational_areas=tuple(sorted(row.subnational_areas or [])),
                    dataset=row.dataset,
                )
            )

    # Sort by ID so the files, and the seeded sample, are stable across runs.
    all_items = sorted(items, key=lambda item: item.id)
    pool = all_items
    if datasets:
        pool = [item for item in all_items if item.dataset in datasets]
        missing = sorted(set(datasets) - {item.dataset for item in pool})
        if missing:
            raise click.UsageError(f"No items for dataset(s): {', '.join(missing)}")
    if sample is None:
        sample = len(pool) if datasets else 50
    if sample > len(pool):
        raise click.UsageError(f"Sample {sample} exceeds {len(pool)} items.")
    rng = random.Random(seed)
    if uniform:
        test_items = rng.sample(pool, sample)
    else:
        test_items = sample_across_datasets(pool, sample, rng)

    if not skip_all:
        write_jsonl(DATA_DIR / "all.jsonl", all_items)
        console.print(f"Wrote {len(all_items)} items to all.jsonl.")
    write_jsonl(DATA_DIR / f"{output}.jsonl", test_items)
    console.print(f"Wrote {len(test_items)} items to {output}.jsonl.")


if __name__ == "__main__":
    main()
