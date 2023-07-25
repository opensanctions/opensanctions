import sys
import click
import logging
from pathlib import Path
from followthemoney.cli.util import InPath

from zavod.logs import configure_logging, get_logger
from zavod.meta import load_dataset_from_path
from zavod.runner import run_dataset
from zavod.tools.load_db import load_dataset_to_db
from zavod.exc import RunFailedException

log = get_logger(__name__)


@click.group(help="Zavod data factory")
def cli() -> None:
    configure_logging(level=logging.INFO)


@cli.command("run", help="Run a specific dataset")
@click.argument("path", type=InPath)
@click.option("-d", "--dry-run", is_flag=True, default=False)
def run(path: Path, dry_run: bool = False) -> None:
    dataset = load_dataset_from_path(path)
    if dataset is None:
        raise RuntimeError("Could not load dataset: %s" % path)
    try:
        run_dataset(dataset, dry_run=dry_run)
    except RunFailedException:
        sys.exit(1)


@cli.command("load-db", help="Load dataset statements from the archive into a database")
@click.argument("database_uri", type=str)
@click.argument("path", type=InPath)
@click.option("--batch-size", type=int, default=5000)
def load_db(database_uri: str, path: Path, batch_size: int = 5000) -> None:
    dataset = load_dataset_from_path(path)
    if dataset is None:
        raise RuntimeError("Could not load dataset: %s" % path)
    load_dataset_to_db(dataset, database_uri, batch_size=batch_size)
