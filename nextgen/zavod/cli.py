import click
import logging
from pathlib import Path
from followthemoney.cli.util import InPath

from zavod.logs import configure_logging, get_logger

log = get_logger(__name__)


@click.group(help="Zavod data factory")
def cli() -> None:
    configure_logging(level=logging.INFO)


@cli.command("run", help="Run a specific dataset crawler")
@click.argument("path", type=InPath)
@click.option("-d", "--dry-run", is_flag=True, default=False)
def run(path: Path, dry_run: bool = False) -> None:
    pass
