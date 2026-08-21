import sys
from pathlib import Path

import click

from zavod import settings
from zavod.archive import clear_data_path
from zavod.cli import cli, DatasetInPath, _load_dataset, log
from zavod.crawl import crawl_dataset
from zavod.exc import RunFailedException
from zavod.exporters import export_dataset
from zavod.integration import get_dataset_linker
from zavod.publish import publish_dataset, archive_failure
from zavod.runtime.versions import make_version
from zavod.store import get_store
from zavod.tools.load_db import load_dataset_to_db


@cli.command("crawl", help="Crawl a specific dataset")
@click.argument("dataset_path", type=DatasetInPath)
@click.option("-d", "--dry-run", is_flag=True, default=False)
@click.option("--clear-data/--keep-data", is_flag=True, default=True)
def crawl(dataset_path: Path, dry_run: bool = False, clear_data: bool = False) -> None:
    dataset = _load_dataset(dataset_path)
    if clear_data:
        clear_data_path(dataset.name)

    try:
        crawl_dataset(dataset, dry_run=dry_run)
    except RunFailedException:
        sys.exit(1)


@cli.command("export", help="Export and validate data from a specific dataset")
@click.argument("dataset_path", type=DatasetInPath)
@click.option("--rebuild-store/--keep-store", is_flag=True, default=True)
@click.option("--validate/--no-validate", is_flag=True, default=True)
def export(
    dataset_path: Path, rebuild_store: bool = True, validate: bool = True
) -> None:
    dataset = _load_dataset(dataset_path)
    if dataset.model.disabled:
        log.info(f"Dataset is disabled, skipping: {dataset.name}")
        sys.exit(0)
    linker = get_dataset_linker(dataset)
    store = get_store(dataset, linker)
    try:
        store.sync(clear=rebuild_store)
        view = store.view(dataset, external=False)
        export_dataset(dataset, view, validate=validate)
    except Exception:
        log.exception(f"Failed to export: {dataset_path}")
        sys.exit(1)
    finally:
        store.close()


@cli.command("publish", help="Publish data from a specific dataset")
@click.argument("dataset_path", type=DatasetInPath)
def publish(dataset_path: Path) -> None:
    dataset = _load_dataset(dataset_path)
    make_version(dataset, settings.RUN_VERSION, append_new_version_to_history=False)
    try:
        publish_dataset(dataset)
    except Exception:
        log.exception(f"Failed to publish: {dataset_path}")
        sys.exit(1)


@cli.command("run", help="Crawl, export and then publish a specific dataset")
@click.argument("dataset_path", type=DatasetInPath)
@click.option("--clear-data/--keep-data", is_flag=True, default=True)
def run(
    dataset_path: Path,
    clear_data: bool = False,
) -> None:
    dataset = _load_dataset(dataset_path)
    if clear_data:
        clear_data_path(dataset.name)

    if dataset.model.disabled:
        log.info(f"Dataset is disabled, skipping: {dataset.name}")
        archive_failure(dataset)
        sys.exit(0)
    # crawl if it's a dataset, just create a new version if it's a collection
    if dataset.model.entry_point is not None and not dataset.is_collection:
        try:
            crawl_dataset(dataset, dry_run=False)
        except RunFailedException:
            archive_failure(dataset)
            sys.exit(1)
    else:
        # crawl_dataset -> Context.begin does this in the case above
        make_version(dataset, settings.RUN_VERSION, append_new_version_to_history=True)

    linker = get_dataset_linker(dataset)
    store = get_store(dataset, linker)
    # Export and validation
    try:
        store.sync(clear=True)
        view = store.view(dataset, external=False)
        export_dataset(dataset, view)
    except Exception:
        log.exception(f"Failed to export: {dataset_path}")
        archive_failure(dataset)
        store.close()
        sys.exit(1)

    # Publish
    try:
        publish_dataset(dataset)

        if not dataset.is_collection and dataset.model.load_statements:
            log.info("Loading dataset into database...", dataset=dataset.name)
            load_dataset_to_db(dataset, linker, external=False)
        log.info("Dataset run is complete :)", dataset=dataset.name)
    except Exception:
        log.exception(f"Failed to publish {dataset.name!r}")
        sys.exit(1)
