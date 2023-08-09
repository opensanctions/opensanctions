import sys
import click
import logging
from pathlib import Path
from followthemoney.cli.util import InPath, OutPath
from nomenklatura.statement import CSV, FORMATS

from zavod.logs import configure_logging, get_logger
from zavod.meta import load_dataset_from_path, Dataset
from zavod.crawl import crawl_dataset
from zavod.store import get_view, clear_store
from zavod.archive import clear_data_path
from zavod.exporters import export_dataset
from zavod.publish import publish_dataset, publish_failure
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


@cli.command("crawl", help="Crawl a specific dataset")
@click.argument("dataset_path", type=InPath)
@click.option("-d", "--dry-run", is_flag=True, default=False)
@click.option("-c", "--clear", is_flag=True, default=False)
def crawl(dataset_path: Path, dry_run: bool = False, clear: bool = False) -> None:
    dataset = _load_dataset(dataset_path)
    if clear:
        clear_data_path(dataset.name)
    try:
        crawl_dataset(dataset, dry_run=dry_run)
    except RunFailedException:
        sys.exit(1)


@cli.command("export", help="Export data from a specific dataset")
@click.argument("dataset_path", type=InPath)
@click.option("-c", "--clear", is_flag=True, default=False)
def export(dataset_path: Path, clear: bool = False) -> None:
    dataset = _load_dataset(dataset_path)
    if clear:
        clear_store(dataset)
    view = get_view(dataset, external=False)
    export_dataset(dataset, view)


@cli.command("publish", help="Publish data from a specific dataset")
@click.argument("dataset_path", type=InPath)
@click.option("-l", "--latest", is_flag=True, default=False)
def publish(dataset_path: Path, latest: bool = False) -> None:
    dataset = _load_dataset(dataset_path)
    publish_dataset(dataset, latest=latest)


@cli.command("run", help="Crawl, export and then publish a specific dataset")
@click.argument("dataset_path", type=InPath)
@click.option("-l", "--latest", is_flag=True, default=False)
@click.option("-c", "--clear", is_flag=True, default=False)
@click.option("-x", "--external", is_flag=True, default=True)
def run(
    dataset_path: Path,
    latest: bool = False,
    clear: bool = False,
    external: bool = False,
) -> None:
    dataset = _load_dataset(dataset_path)
    if clear:
        clear_data_path(dataset.name)
    if dataset.entry_point is not None and not dataset.is_collection:
        try:
            crawl_dataset(dataset, dry_run=False)
        except RunFailedException:
            publish_failure(dataset, latest=latest)
            sys.exit(1)
    view = get_view(dataset, external=False)
    export_dataset(dataset, view)
    publish_dataset(dataset, latest=latest)

    if not dataset.is_collection and dataset.load_db_uri is not None:
        log.info("Loading dataset into database...")
        load_dataset_to_db(dataset, dataset.load_db_uri, external=external)


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


@cli.command("clear", help="Delete the data and state paths for a dataset")
@click.argument("dataset_path", type=InPath)
def clear(dataset_path: Path) -> None:
    dataset = _load_dataset(dataset_path)
    clear_data_path(dataset.name)
