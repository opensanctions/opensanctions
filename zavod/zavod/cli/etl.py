import sys
from pathlib import Path

import click

from zavod import settings
from zavod.archive import clear_data_path, create_artifact_path
from zavod.cli import cli, DatasetInPath, _load_dataset, log
from zavod.crawl import crawl_dataset
from zavod.exc import RunFailedException
from zavod.exporters import export_dataset
from zavod.integration import get_dataset_linker
from zavod.publish import publish_dataset, archive_failure
from zavod.store import get_store
from zavod.tools.load_db import load_dataset_to_db


@cli.command("crawl", help="Crawl a specific dataset")
@click.argument("dataset_path", type=DatasetInPath)
@click.option("-v", "--version", envvar="ZAVOD_VERSION", default=None, show_envvar=True)
@click.option("-d", "--dry-run", is_flag=True, default=False)
@click.option("--clear-data/--keep-data", is_flag=True, default=True)
def crawl(
    dataset_path: Path,
    version: str | None = None,
    dry_run: bool = False,
    clear_data: bool = False,
) -> None:
    dataset = _load_dataset(dataset_path)
    if clear_data:
        clear_data_path(dataset.name)

    if version is None:
        version = settings.RUN_VERSION.id

    try:
        crawl_dataset(dataset, version=version, dry_run=dry_run)
    except RunFailedException:
        sys.exit(1)


@cli.command("export", help="Export and validate data from a specific dataset")
@click.argument("dataset_path", type=DatasetInPath)
@click.argument("version", envvar="ZAVOD_VERSION", type=str)
@click.option("--rebuild-store/--keep-store", is_flag=True, default=True)
@click.option("--validate/--no-validate", is_flag=True, default=True)
def export(
    dataset_path: Path, version: str, rebuild_store: bool = True, validate: bool = True
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
        export_dataset(dataset, version, view, validate=validate)
    except Exception:
        log.exception(f"Failed to export: {dataset_path}")
        sys.exit(1)
    finally:
        store.close()


@cli.command("publish", help="Publish data from a specific dataset")
@click.argument("dataset_path", type=DatasetInPath)
@click.argument("version", type=str)
def publish(dataset_path: Path, version: str) -> None:
    dataset = _load_dataset(dataset_path)
    try:
        publish_dataset(dataset, version)
    except Exception:
        log.exception(f"Failed to publish: {dataset_path}")
        sys.exit(1)


@cli.command("run", help="Crawl, export and then publish a specific dataset")
@click.argument("dataset_path", type=DatasetInPath)
@click.option("-v", "--version", envvar="ZAVOD_VERSION", default=None, show_envvar=True)
@click.option("--clear-data/--keep-data", is_flag=True, default=True)
def run(
    dataset_path: Path,
    version: str | None = None,
    clear_data: bool = False,
) -> None:
    dataset = _load_dataset(dataset_path)
    if clear_data:
        clear_data_path(dataset.name)

    if dataset.model.disabled:
        log.info(f"Dataset is disabled, skipping: {dataset.name}")
        # archive_failure(dataset)
        sys.exit(0)

    if version is None:
        version = settings.RUN_VERSION.id

    # crawl if it's a dataset, just create a new version if it's a collection
    if dataset.model.entry_point is not None and not dataset.is_collection:
        try:
            crawl_dataset(dataset, version=version, dry_run=False)
        except RunFailedException:
            archive_failure(dataset, version)
            sys.exit(1)
    else:
        create_artifact_path(dataset.name, version)

    linker = get_dataset_linker(dataset)
    store = get_store(dataset, linker)
    # Export and validation
    try:
        store.sync(clear=True)
        view = store.view(dataset, external=False)
        export_dataset(dataset, version, view)
    except Exception:
        log.exception(f"Failed to export: {dataset_path}")
        archive_failure(dataset, version)
        store.close()
        sys.exit(1)

    # Publish
    try:
        publish_dataset(dataset, version)

        if not dataset.is_collection and dataset.model.load_statements:
            log.info("Loading dataset into database...", dataset=dataset.name)
            load_dataset_to_db(dataset, linker, external=False)
        log.info("Dataset run is complete :)", dataset=dataset.name)
    except Exception:
        log.exception(f"Failed to publish {dataset.name!r}")
        sys.exit(1)
