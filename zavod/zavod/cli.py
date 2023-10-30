import sys
import click
import logging
from pathlib import Path
from typing import Optional, List
from followthemoney.cli.util import InPath, OutPath
from nomenklatura.tui import dedupe_ui
from nomenklatura.statement import CSV, FORMATS
from nomenklatura.matching import DefaultAlgorithm

from zavod import settings
from zavod.logs import configure_logging, get_logger
from zavod.meta import load_dataset_from_path, get_multi_dataset, Dataset
from zavod.crawl import crawl_dataset
from zavod.runtime.cache import get_cache
from zavod.store import get_view, get_store, clear_store
from zavod.archive import clear_data_path
from zavod.exporters import export_dataset
from zavod.dedupe import get_resolver, blocking_xref, merge_entities
from zavod.dedupe import explode_cluster
from zavod.publish import publish_dataset, publish_failure
from zavod.tools.load_db import load_dataset_to_db
from zavod.tools.dump_file import dump_dataset_to_file
from zavod.exc import RunFailedException
from zavod.wd_up import run_app


log = get_logger(__name__)
STMT_FORMATS = click.Choice(FORMATS, case_sensitive=False)


def _load_dataset(path: Path) -> Dataset:
    dataset = load_dataset_from_path(path)
    if dataset is None:
        raise click.BadParameter("Invalid dataset path: %s" % path)
    return dataset


def _load_datasets(paths: List[Path]) -> Dataset:
    inputs: List[str] = []
    for path in paths:
        inputs.append(_load_dataset(path).name)
    return get_multi_dataset(inputs)


@click.group(help="Zavod data factory")
@click.option("--debug", is_flag=True, default=False)
def cli(debug: bool = False) -> None:
    settings.DEBUG = debug
    level = logging.DEBUG if debug else logging.INFO
    configure_logging(level=level)


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
    try:
        dataset = _load_dataset(dataset_path)
        if clear:
            clear_store(dataset)
        view = get_view(dataset, external=False)
        export_dataset(dataset, view)
    except Exception:
        log.exception("Failed to export: %s" % dataset_path)
        sys.exit(1)


@cli.command("publish", help="Publish data from a specific dataset")
@click.argument("dataset_path", type=InPath)
@click.option("-l", "--latest", is_flag=True, default=False)
def publish(dataset_path: Path, latest: bool = False) -> None:
    try:
        dataset = _load_dataset(dataset_path)
        publish_dataset(dataset, latest=latest)
    except Exception:
        log.exception("Failed to publish: %s" % dataset_path)
        sys.exit(1)


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
    if dataset.disabled:
        log.info("Dataset is disabled, skipping: %s" % dataset.name)
        publish_failure(dataset, latest=latest)
        sys.exit(0)
    if dataset.entry_point is not None and not dataset.is_collection:
        try:
            crawl_dataset(dataset, dry_run=False)
        except RunFailedException:
            publish_failure(dataset, latest=latest)
            sys.exit(1)
    try:
        clear_store(dataset)
        view = get_view(dataset, external=False)
        export_dataset(dataset, view)
        view.store.close()
        publish_dataset(dataset, latest=latest)

        if not dataset.is_collection and dataset.load_db_uri is not None:
            log.info("Loading dataset into database...", dataset=dataset.name)
            load_dataset_to_db(dataset, dataset.load_db_uri, external=external)
        log.info("Dataset run is complete :)", dataset=dataset.name)
    except Exception:
        log.exception("Failed to export and publish %r" % dataset.name)
        sys.exit(1)


@cli.command("load-db", help="Load dataset statements from the archive into a database")
@click.argument("dataset_path", type=InPath)
@click.argument("database_uri", type=str)
@click.option("--batch-size", type=int, default=settings.DB_BATCH_SIZE)
@click.option("-x", "--external", is_flag=True, default=False)
def load_db(
    dataset_path: Path,
    database_uri: str,
    batch_size: int = 5000,
    external: bool = False,
) -> None:
    try:
        dataset = _load_dataset(dataset_path)
        load_dataset_to_db(
            dataset,
            database_uri,
            batch_size=batch_size,
            external=external,
        )
    except Exception:
        log.exception("Failed to load dataset into database: %s" % dataset_path)
        sys.exit(1)


@cli.command("dump-file", help="Dump dataset statements from the archive to a file")
@click.argument("dataset_path", type=InPath)
@click.argument("out_path", type=OutPath)
@click.option("-f", "--format", type=STMT_FORMATS, default=CSV)
@click.option("-x", "--external", is_flag=True, default=False)
def dump_file(
    dataset_path: Path, out_path: Path, format: str, external: bool = False
) -> None:
    try:
        dataset = _load_dataset(dataset_path)
        dump_dataset_to_file(
            dataset,
            out_path,
            format=format.lower(),
            external=external,
        )
    except Exception:
        log.exception("Failed to dump dataset to file: %s" % dataset_path)
        sys.exit(1)


@cli.command("xref", help="Generate dedupe candidates from the given dataset")
@click.argument("dataset_paths", type=InPath, nargs=-1)
@click.option("-c", "--clear", is_flag=True, default=False)
@click.option("-l", "--limit", type=int, default=10000)
@click.option("-f", "--focus-dataset", type=str, default=None)
@click.option("-s", "--schema", type=str, default=None)
@click.option("-a", "--algorithm", type=str, default=DefaultAlgorithm.NAME)
@click.option("-t", "--threshold", type=float, default=None)
def xref(
    dataset_paths: List[Path],
    clear: bool,
    limit: int,
    threshold: Optional[float],
    algorithm: str,
    focus_dataset: Optional[str] = None,
    schema: Optional[str] = None,
) -> None:
    dataset = _load_datasets(dataset_paths)
    if clear:
        clear_store(dataset)
    store = get_store(dataset, external=True)
    blocking_xref(
        store,
        limit=limit,
        auto_threshold=threshold,
        algorithm=algorithm,
        focus_dataset=focus_dataset,
        schema_range=schema,
    )


@cli.command("resolver-prune", help="Remove dedupe candidates from resolver file")
def xref_prune() -> None:
    try:
        resolver = get_resolver()
        resolver.prune()
        resolver.save()
    except Exception:
        log.exception("Failed to prune resolver file")
        sys.exit(1)


@cli.command("dedupe", help="Interactively decide xref candidates")
@click.argument("dataset_paths", type=InPath, nargs=-1)
@click.option("-c", "--clear", is_flag=True, default=False)
def dedupe(dataset_paths: List[Path], clear: bool = False) -> None:
    dataset = _load_datasets(dataset_paths)
    if clear:
        clear_store(dataset)
    store = get_store(dataset, external=True)
    dedupe_ui(store, url_base="https://opensanctions.org/entities/%s/")


@cli.command("explode-cluster", help="Destroy a cluster of deduplication matches")
@click.argument("canonical_id", type=str)
def explode(canonical_id: str) -> None:
    explode_cluster(canonical_id)


@cli.command("merge-cluster", help="Merge multiple entities as duplicates")
@click.argument("entity_ids", type=str, nargs=-1)
@click.option("-f", "--force", is_flag=True, default=False)
def merge(entity_ids: List[str], force: bool = False) -> None:
    try:
        merge_entities(entity_ids, force=force)
    except ValueError as ve:
        log.error("Cannot merge: %s" % ve)
        sys.exit(1)


@cli.command("clear", help="Delete the data and state paths for a dataset")
@click.argument("dataset_path", type=InPath)
def clear(dataset_path: Path) -> None:
    try:
        dataset = _load_dataset(dataset_path)
        clear_data_path(dataset.name)
    except Exception:
        log.exception("Failed to clear dataset: %s" % dataset_path)
        sys.exit(1)


@cli.command("wd-up", help="Review and output possible wikidata updates")
@click.argument("dataset_paths", type=InPath, nargs=-1)
@click.option("-o", "--out-file", type=str, required=True)
@click.option("-c", "--clear", is_flag=True, default=False)
@click.option("-f", "--focus-dataset", type=str, default=None)
def wd_up(
    dataset_paths: List[Path],
    out_file: str,
    clear: bool,
    focus_dataset: Optional[str] = None,
) -> None:
    dataset = _load_datasets(dataset_paths)
    if clear:
        clear_store(dataset)
    view = get_view(dataset, external=False)
    cache = get_cache(dataset)
    run_app(
        out_file,
        view,
        cache,
        focus_dataset=focus_dataset,
    )
