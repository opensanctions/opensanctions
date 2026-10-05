from pathlib import Path

import click
from followthemoney.dataset import Version

from zavod.archive import STATEMENTS_FILE, get_version_history
from zavod.cli import cli, DatasetInPath, _load_dataset
from zavod.runtime.urls import make_artifact_url


@cli.group("archive", help="Archive-related utilities")
def archive() -> None:
    pass


@archive.command("url", help="Print the public URL for a dataset resource")
@click.argument("resource_filename", type=click.Choice([STATEMENTS_FILE]))
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

    history = get_version_history(dataset.name)
    version: Version | None = None
    if latest:
        version = history.latest
    elif last_successful:
        version = history.last_successful
    if version is None:
        raise click.ClickException(f"No matching version found for: {dataset.name}")

    url = make_artifact_url(dataset.name, version, resource_filename)
    click.echo(url)
