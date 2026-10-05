import os
import random
from datetime import datetime

import click
from rich.console import Console
from sqlalchemy import (
    JSON,
    Column,
    DateTime,
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
    Column("caption", Unicode),
    Column("countries", JSON),
    Column("subnational_areas", JSON),
    Column("dataset", Unicode),
    Column("created_at", DateTime),
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


@click.command()
@click.option(
    "--sample", default=50, show_default=True, help="Size of the test subset."
)
@click.option("--seed", default=0, show_default=True, help="Seed for the test subset.")
@click.option(
    "--since",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default="2026-07-01",
    show_default=True,
    help="Only load positions first seen after this date.",
)
def main(sample: int, seed: int, since: datetime) -> None:
    """Load all non-Wikidata position items into data/all.jsonl and sample data/test.jsonl."""
    table = position_table
    query = select(
        table.c.caption, table.c.countries, table.c.subnational_areas, table.c.dataset
    ).where(
        table.c.deleted_at.is_(None),
        table.c.created_at > since,
        ~table.c.dataset.like("wd\\_%", escape="\\"),
    )
    items: set[Item] = set()
    engine = create_engine(get_database_uri())
    with console.status("Reading positions"), engine.connect() as conn:
        for row in conn.execute(query):
            items.add(
                Item(
                    caption=row.caption,
                    countries=tuple(sorted(row.countries)),
                    subnational_areas=tuple(sorted(row.subnational_areas or [])),
                    dataset=row.dataset,
                )
            )

    # Sort by ID so the files, and the seeded sample, are stable across runs.
    all_items = sorted(items, key=lambda item: item.id)
    if sample > len(all_items):
        raise click.UsageError(f"Sample {sample} exceeds {len(all_items)} items.")
    test_items = random.Random(seed).sample(all_items, sample)

    write_jsonl(DATA_DIR / "all.jsonl", all_items)
    write_jsonl(DATA_DIR / "test.jsonl", test_items)
    console.print(
        f"Wrote {len(all_items)} items to all.jsonl and {sample} to test.jsonl."
    )


if __name__ == "__main__":
    main()
