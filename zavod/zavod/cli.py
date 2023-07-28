import sys
import click
import logging
from pathlib import Path
from followthemoney.cli.util import InPath, OutPath
from nomenklatura.statement import CSV, FORMATS

from zavod.logs import configure_logging, get_logger
from zavod.meta import load_dataset_from_path, Dataset
from zavod.runner import run_dataset
from zavod.tools.load_db import load_dataset_to_db
from zavod.tools.dump_file import dump_dataset_to_file
from zavod.exc import RunFailedException

log = get_logger(__name__)
STMT_FORMATS = click.Choice(FORMATS, case_sensitive=False)


def _load_dataset(path: Path) -> Dataset:
    dataset = load_dataset_from_path(path)
    if dataset is None:
        raise click.BadParameter("Invalid dataset path: %s" % path)
    return dataset


@click.group(help="Zavod data factory")
def cli() -> None:
    configure_logging(level=logging.INFO)


@cli.command("run", help="Run a specific dataset")
@click.argument("dataset_path", type=InPath)
@click.option("-d", "--dry-run", is_flag=True, default=False)
def run(dataset_path: Path, dry_run: bool = False) -> None:
    dataset = _load_dataset(dataset_path)
    try:
        run_dataset(dataset, dry_run=dry_run)
    except RunFailedException:
        sys.exit(1)


@cli.command("load-db", help="Load dataset statements from the archive into a database")
@click.argument("dataset_path", type=InPath)
@click.argument("database_uri", type=str)
@click.option("--batch-size", type=int, default=5000)
@click.option("-x", "--external", is_flag=True, default=False)
def load_db(
    dataset_path: Path,
    database_uri: str,
    batch_size: int = 5000,
    external: bool = False,
) -> None:
    dataset = _load_dataset(dataset_path)
    load_dataset_to_db(dataset, database_uri, batch_size=batch_size, external=external)


@cli.command("dump-file", help="Dump dataset statements from the archive to a file")
@click.argument("dataset_path", type=InPath)
@click.argument("out_path", type=OutPath)
@click.option("-f", "--format", type=STMT_FORMATS, default=CSV)
@click.option("-x", "--external", is_flag=True, default=False)
def dump_file(
    dataset_path: Path, out_path: Path, format: str, external: bool = False
) -> None:
    dataset = _load_dataset(dataset_path)
    dump_dataset_to_file(dataset, out_path, format=format.lower(), external=external)
