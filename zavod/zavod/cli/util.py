import sys
from pathlib import Path
from typing import Optional, List

import click
from followthemoney.cli.util import OutPath
from followthemoney.statement import CSV
from nomenklatura.settings import STATEMENT_BATCH

from zavod.archive import clear_data_path
from zavod.cli import cli, DatasetInPath, STMT_FORMATS, load_dataset, log
from zavod.integration import get_dataset_linker
from zavod.store import get_store
from zavod.tools.dump_file import dump_dataset_to_file
from zavod.tools.load_db import load_dataset_to_db
from zavod.tools.summarize import summarize as _summarize


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
        dataset = load_dataset(dataset_path)
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
        dataset = load_dataset(dataset_path)
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
        dataset = load_dataset(dataset_path)
        clear_data_path(dataset.name)
    except Exception:
        log.exception("Failed to clear dataset: %s" % dataset_path)
        sys.exit(1)


@cli.command("summarize")
@click.argument("dataset_path", type=DatasetInPath)
@click.option("-r", "--rebuild-store", is_flag=True, default=False)
@click.option("-s", "--schema", type=str, default=None)
@click.option(
    "-f",
    "--from-prop",
    type=str,
    default=None,
    help="The property from the initial entity referring to the linking entity",
)
@click.option(
    "-l",
    "--link-props",
    type=str,
    default=[],
    multiple=True,
    help="The properties of the linking entity to show",
)
@click.option(
    "-t",
    "--to-prop",
    type=str,
    default=None,
    help="The property from the linking entity referring to the final entity",
)
@click.option(
    "-p",
    "--to-props",
    type=str,
    default=[],
    multiple=True,
    help="The properties of the final entity to show",
)
def summarize(
    dataset_path: Path,
    rebuild_store: bool = False,
    schema: Optional[str] = None,
    from_prop: Optional[str] = None,
    link_props: List[str] = [],
    to_prop: Optional[str] = None,
    to_props: List[str] = [],
) -> None:
    """Summarise entities and links in a dataset

    Example to summarise the positions held by people in a dataset of political entities:

    \b
    zavod summarize \\
        --schema Person \\
        --from-prop positionOccupancies \\
        --link-props startDate \\
        --link-props endDate \\
        --to-prop post \\
        datasets/ng/join_dots/ng_join_dots.yml
    """
    try:
        dataset = load_dataset(dataset_path)
        linker = get_dataset_linker(dataset)
        store = get_store(dataset, linker)
        store.sync(clear=rebuild_store)
        view = store.view(dataset, external=False)
        _summarize(view, schema, from_prop, link_props, to_prop, to_props)
    except Exception:
        log.exception("Failed to summarize: %s" % dataset_path)
        sys.exit(1)
