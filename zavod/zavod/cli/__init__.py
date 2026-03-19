import logging
from pathlib import Path
from typing import List

import click
from click.shell_completion import CompletionItem

from followthemoney.statement import FORMATS
from zavod import settings
from zavod.logs import configure_logging, get_logger, set_logging_context_dataset_name
from zavod.meta import load_dataset_from_path, get_multi_dataset, Dataset
from zavod.stateful.model import create_db

log = get_logger(__name__)
STMT_FORMATS = click.Choice(FORMATS, case_sensitive=False)


# Activate shell completions by putting the following in your zsh direnv .envrc
# This will cache the completion function for 30 days to now slow shell startup down
# (){ [[ $# -gt 0 ]] || _ZAVOD_COMPLETE=zsh_source zavod > .zavod-complete.zsh; source .zavod-complete.zsh; } .zavod-complete.zsh(Nm-30)
# Alternatively, the slow way that does not cache is
# eval "$(_ZAVOD_COMPLETE=zsh_source zavod)"
class DatasetPath(click.Path):
    """Custom Click parameter type for dataset paths that provides shell completion."""

    def shell_complete(
        self, ctx: click.Context, param: click.Parameter, incomplete: str
    ) -> List[CompletionItem]:
        completions: list[CompletionItem] = []
        for path in sorted(Path("datasets").glob("**/*.yml")):
            if incomplete in path.name:
                completions.append(CompletionItem(str(path)))
        return completions


DatasetInPath = DatasetPath(
    dir_okay=False, readable=True, path_type=Path, allow_dash=True
)


def _load_dataset(path: Path) -> Dataset:
    dataset = load_dataset_from_path(path)
    if dataset is None:
        raise click.BadParameter("Invalid dataset path: %s" % path)
    set_logging_context_dataset_name(dataset.name)
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
    create_db()


# Register submodule commands
from zavod.cli import etl as _etl  # noqa: E402, F401
from zavod.cli import dedupe as _dedupe  # noqa: E402, F401
from zavod.cli import util as _util  # noqa: E402, F401
from zavod.cli import wd_up as _wd_up  # noqa: E402, F401
from zavod.cli import archive as _archive  # noqa: E402, F401
