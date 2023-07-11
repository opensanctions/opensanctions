import click
import logging

from zavod.logs import configure_logging, get_logger

log = get_logger(__name__)


@click.group(help="Zavod data factory")
def cli() -> None:
    configure_logging(level=logging.INFO)
