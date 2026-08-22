from pathlib import Path

import click

from zavod.archive import (
    STATEMENTS_FILE,
    get_last_successful_version,
    iter_dataset_versions,
)
from zavod.cli import cli, DatasetInPath, _load_dataset
from zavod.meta.dataset import Dataset
from zavod.runtime.urls import make_artifact_url

RESOURCE_FILENAMES = [STATEMENTS_FILE]


def _get_latest_version(dataset: Dataset) -> str:
    # iter_dataset_versions always reads from the archive, never from the local file system
    # which is what we want in this case. Otherwise we might end up with a version that's not
    # actually in the archive but just from a local run.
    for v in iter_dataset_versions(dataset.name):
        return v.id
    raise click.ClickException(f"No version history found for dataset: {dataset.name}")


@cli.group("archive", help="Archive-related utilities")
def archive() -> None:
    pass


@archive.command("url", help="Print the public URL for a dataset resource")
@click.argument("resource_filename", type=click.Choice(RESOURCE_FILENAMES))
@click.argument("dataset_path", type=DatasetInPath)
@click.option(
    "--latest",
    is_flag=True,
    default=False,
    help="Resolve the latest version from versions.json",
)
@click.option(
    "--last-successful",
    is_flag=True,
    default=False,
    help="Resolve the URL for the last successful version.",
)
def url(
    resource_filename: str,
    dataset_path: Path,
    latest: bool = False,
    last_successful: bool = False,
) -> None:
    dataset = _load_dataset(dataset_path)

    if sum([latest, last_successful]) != 1:
        # No support for finding other versions yet
        raise click.ClickException(
            "Exactly one of --latest or --last-successful is required."
        )

    version: str | None = None
    if latest:
        version = _get_latest_version(dataset)
    elif last_successful:
        version = get_last_successful_version(dataset.name)
        if version is None:
            raise click.ClickException(
                f"No last successful version found for dataset: {dataset.name}"
            )

    assert version is not None

    url = make_artifact_url(dataset.name, version, resource_filename)
    click.echo(url)
