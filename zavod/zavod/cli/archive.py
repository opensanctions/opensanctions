from pathlib import Path

import click

from zavod import settings
from zavod.archive import ARTIFACTS, STATEMENTS_FILE
from zavod.cli import cli, DatasetInPath, _load_dataset
from zavod.runtime.versions import get_latest

RESOURCE_TYPES = {
    "statements": STATEMENTS_FILE,
}


@cli.group("archive", help="Archive-related utilities")
def archive() -> None:
    pass


@archive.command("url", help="Print the public URL for a dataset resource")
@click.argument("resource_type", type=click.Choice(list(RESOURCE_TYPES)))
@click.argument("dataset_path", type=DatasetInPath)
@click.option(
    "--latest",
    is_flag=True,
    default=False,
    help="Resolve the latest version from versions.json",
)
def url(resource_type: str, dataset_path: Path, latest: bool = False) -> None:
    dataset = _load_dataset(dataset_path)
    filename = RESOURCE_TYPES[resource_type]

    if not latest:
        # No support for finding other versions yet
        raise click.ClickException("--latest is required.")

    if latest:
        version = get_latest(dataset.name)
        if version is None:
            raise click.ClickException(
                f"No version history found for dataset: {dataset.name}"
            )
        click.echo(
            f"{settings.ARCHIVE_SITE}/{ARTIFACTS}/{dataset.name}/{version}/{filename}"
        )
