import sys
from pathlib import Path

import click
from followthemoney.cli.util import OutPath
from followthemoney.statement import CSV
from nomenklatura.settings import STATEMENT_BATCH

from zavod.archive import clear_data_path
from zavod.cli import cli, DatasetInPath, STMT_FORMATS, _load_dataset, log
from zavod.integration import get_dataset_linker
from zavod.tools.dump_file import dump_dataset_to_file
from zavod.tools.load_db import load_dataset_to_db


@cli.command("load-db", help="Load dataset statements from the archive into a database")
@click.argument("dataset_path", type=DatasetInPath)
@click.option("--batch-size", type=int, default=STATEMENT_BATCH)
@click.option("-x", "--external", is_flag=True, default=False)
def load_db(
    dataset_path: Path,
    batch_size: int = 5000,
    external: bool = False,
) -> None:
    try:
        dataset = _load_dataset(dataset_path)
        linker = get_dataset_linker(dataset)
        load_dataset_to_db(
            dataset,
            linker,
            batch_size=batch_size,
            external=external,
        )
    except Exception:
        log.exception("Failed to load dataset into database: %s" % dataset_path)
        sys.exit(1)


@cli.command("dump-file", help="Dump dataset statements from the archive to a file")
@click.argument("dataset_path", type=DatasetInPath)
@click.argument("out_path", type=OutPath)
@click.option("-f", "--format", type=STMT_FORMATS, default=CSV)
@click.option("-x", "--external", is_flag=True, default=False)
def dump_file(
    dataset_path: Path, out_path: Path, format: str, external: bool = False
) -> None:
    try:
        dataset = _load_dataset(dataset_path)
        linker = get_dataset_linker(dataset)
        dump_dataset_to_file(
            dataset,
            linker,
            out_path,
            format=format.lower(),
            external=external,
        )
    except Exception:
        log.exception("Failed to dump dataset to file: %s" % dataset_path)
        sys.exit(1)


@cli.command("clear", help="Delete the data and state paths for a dataset")
@click.argument("dataset_path", type=DatasetInPath)
def clear(dataset_path: Path) -> None:
    try:
        dataset = _load_dataset(dataset_path)
        clear_data_path(dataset.name)
    except Exception:
        log.exception("Failed to clear dataset: %s" % dataset_path)
        sys.exit(1)
