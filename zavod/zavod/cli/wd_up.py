from pathlib import Path
from typing import Optional, List

import click

from zavod.cli import cli, DatasetInPath, load_datasets
from zavod.integration import get_resolver
from zavod.store import get_store


@cli.command("wd-up")
@click.argument("dataset_paths", type=DatasetInPath, nargs=-1)
@click.option("-r", "--rebuild-store", is_flag=True, default=False)
@click.option("-a", "--country-adjective", type=str, required=True)
@click.option("-d", "--country-code", type=str, required=True)
@click.option("-f", "--focus-dataset", type=str, default=None)
def wd_up(
    dataset_paths: List[Path],
    rebuild_store: bool,
    country_code: str,
    country_adjective: str,
    focus_dataset: Optional[str] = None,
) -> None:
    """Interactively review and apply wikidata updates from OpenSanctions data.

    Example:

    \b
    zavod wd-up \\
        --rebuild-store \\
        datasets/de/abgeordnetenwatch/de_abgeordnetenwatch.yml \\
        datasets/_analysis/ann_pep_positions/ann_pep_positions.yml \\
        --country-adjective German \\
        --country-code de
    """
    from zavod.tools.wikidata import run_app

    dataset = load_datasets(dataset_paths)
    resolver = get_resolver()
    resolver.begin()
    store = get_store(dataset, resolver)
    store.sync(clear=rebuild_store)
    run_app(
        resolver,
        store,
        country_code=country_code,
        country_adjective=country_adjective,
        focus_dataset=focus_dataset,
    )
    resolver.commit()
