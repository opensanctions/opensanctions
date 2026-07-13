import sys
from pathlib import Path

import click
from followthemoney.dataset import Version

from zavod import settings
from zavod.archive import clear_data_path, latest_local_version
from zavod.cli import cli, DatasetInPath, _load_dataset, log
from zavod.crawl import crawl_dataset
from zavod.exc import RunFailedException
from zavod.exporters import export_dataset
from zavod.integration import get_dataset_linker
from zavod.meta import Dataset
from zavod.publish import publish_dataset, archive_failure
from zavod.runtime.versions import make_version, set_last_successful_version
from zavod.store import get_store
from zavod.tools.load_db import load_dataset_to_db
from zavod.validators import validate_dataset


def _get_latest_local_version(dataset: Dataset) -> Version:
    """The latest local run of the dataset, used when no version is given
    explicitly."""
    version = latest_local_version(dataset.name)
    if version is None:
        raise click.ClickException(
            f"No local run found for dataset: {dataset.name}. Run a crawl first."
        )
    log.info(
        "Operating on the latest local version",
        dataset=dataset.name,
        version=version.id,
    )
    return version


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


@cli.command("validate", help="Check the integrity of a dataset")
@click.argument("dataset_path", type=DatasetInPath)
@click.option("--rebuild-store/--keep-store", is_flag=True, default=True)
def validate(dataset_path: Path, rebuild_store: bool = True) -> None:
    dataset = _load_dataset(dataset_path)
    if dataset.model.disabled:
        log.info("Dataset is disabled, skipping: %s" % dataset.name)
        sys.exit(0)
    version = _get_latest_local_version(dataset)
    linker = get_dataset_linker(dataset)
    store = get_store(dataset, linker, version=version)
    try:
        store.sync(clear=rebuild_store)
        validate_dataset(dataset, store.view(dataset, external=False), version)
    except Exception:
        log.exception("Validation failed for %r" % dataset_path)
        store.close()
        sys.exit(1)


@cli.command("export", help="Export data from a specific dataset")
@click.argument("dataset_path", type=DatasetInPath)
@click.option("--rebuild-store/--keep-store", is_flag=True, default=True)
def export(dataset_path: Path, rebuild_store: bool = True) -> None:
    dataset = _load_dataset(dataset_path)
    if dataset.model.disabled:
        log.info("Dataset is disabled, skipping: %s" % dataset.name)
        sys.exit(0)
    version = _get_latest_local_version(dataset)
    linker = get_dataset_linker(dataset)
    store = get_store(dataset, linker, version=version)
    try:
        store.sync(clear=rebuild_store)
        export_dataset(dataset, store.view(dataset, external=False), version)
    except Exception:
        log.exception("Failed to export: %s" % dataset_path)
        sys.exit(1)


@cli.command("publish", help="Publish data from a specific dataset")
@click.argument("dataset_path", type=DatasetInPath)
@click.option("-l", "--latest", is_flag=True, default=False)
def publish(dataset_path: Path, latest: bool = False) -> None:
    dataset = _load_dataset(dataset_path)
    version = _get_latest_local_version(dataset)
    try:
        publish_dataset(dataset, version, republish_to_latest=latest)
    except Exception:
        log.exception("Failed to publish: %s" % dataset_path)
        sys.exit(1)


@cli.command("run", help="Crawl, export and then publish a specific dataset")
@click.argument("dataset_path", type=DatasetInPath)
@click.option(
    "-l",
    "--latest",
    is_flag=True,
    default=False,
    help="Whether to re-publish to /datasets/latest/, in addition to the timestamped/versioned prefixes.",
)
@click.option("--clear-data/--keep-data", is_flag=True, default=True)
def run(
    dataset_path: Path,
    latest: bool = False,
    clear_data: bool = False,
) -> None:
    dataset = _load_dataset(dataset_path)
    if clear_data:
        clear_data_path(dataset.name)

    # The whole run operates on a single new version, minted here.
    version = settings.RUN_VERSION

    if dataset.model.disabled:
        log.info("Dataset is disabled, skipping: %s" % dataset.name)
        make_version(dataset, version)
        archive_failure(dataset, version)
        sys.exit(0)
    # crawl if it's a dataset, just create a new version if it's a collection
    if dataset.model.entry_point is not None and not dataset.is_collection:
        try:
            crawl_dataset(dataset, dry_run=False, version=version)
        except RunFailedException:
            archive_failure(dataset, version)
            sys.exit(1)
    else:
        # crawl_dataset does this in the case above
        make_version(dataset, version)

    linker = get_dataset_linker(dataset)
    store = get_store(dataset, linker, version=version)
    # Validate
    try:
        store.sync(clear=True)
        view = store.view(dataset, external=False)
        if not dataset.is_collection:
            validate_dataset(dataset, view, version)
    except Exception:
        log.exception("Validation failed for %r" % dataset.name)
        archive_failure(dataset, version)
        store.close()
        sys.exit(1)

    # Export
    try:
        export_dataset(dataset, view, version)
        # Set the version as successful in the version file, which will be archived by publish_dataset.
        set_last_successful_version(dataset, version)
    except Exception:
        log.exception("Failed to export: %s" % dataset_path)
        archive_failure(dataset, version)
        store.close()
        sys.exit(1)

    # Publish
    try:
        publish_dataset(dataset, version, republish_to_latest=latest)

        if not dataset.is_collection and dataset.model.load_statements:
            log.info("Loading dataset into database...", dataset=dataset.name)
            load_dataset_to_db(dataset, linker, external=False)
        log.info("Dataset run is complete :)", dataset=dataset.name)
    except Exception:
        log.exception("Failed to publish %r" % dataset.name)
        sys.exit(1)
