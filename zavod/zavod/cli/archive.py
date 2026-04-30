from pathlib import Path

import click

from zavod import settings
from zavod.archive import (
    ARTIFACTS,
    STATEMENTS_FILE,
    get_versions_data,
    iter_dataset_versions,
)
from zavod.cli import cli, DatasetInPath, _load_dataset
from zavod.meta.dataset import Dataset
from nomenklatura.versions import VersionHistory

RESOURCE_FILENAMES = [STATEMENTS_FILE]


def _get_latest_version(dataset: Dataset) -> str:
    # iter_dataset_versions always reads from the archive, never from the local file system
    # which is what we want in this case. Otherwise we might end up with a version that's not
    # actually in the archive but just from a local run.
    for v in iter_dataset_versions(dataset.name):
        return v.id
    raise click.ClickException(f"No version history found for dataset: {dataset.name}")


def get_last_successful_version(dataset_name: str) -> str | None:
    # TODO(Leon Handreke): We should really clean up the version mess. The fact that we're instantiating
    # VersionHistory from nomenklatura here in the CLI is a code smell.
    # We have some code in archive.py to read the "root" versions file (which is
    # what we want here), and some code in versions.py to read the version file
    # for a specific version with backfilling. We should really make the
    # semantics more intuitive, document more and make it less likely we'll
    # shoot ourselves in the foot in the future.
    history = VersionHistory.from_json(get_versions_data(dataset_name) or "{}")
    if history.last_successful:
        return history.last_successful.id
    return None


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

    click.echo(
        f"{settings.ARCHIVE_SITE}/{ARTIFACTS}/{dataset.name}/{version}/{resource_filename}"
    )
