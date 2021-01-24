import sys
import json
import click
import logging
from followthemoney.cli.util import write_object

from opensanctions.core import Dataset


@click.group(help="OpenSanctions ETL toolkit")
def cli():
    fmt = "%(name)s [%(levelname)s] %(message)s"
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format=fmt)


@cli.command("dump", help="Export the entities from a dataset")
@click.argument("dataset", type=click.Choice(Dataset.names()))
@click.option("-o", "--outfile", type=click.File("w"), default="-")  # noqa
def dump_dataset(dataset, outfile):
    dataset = Dataset.get(dataset)
    for entity in dataset.store:
        write_object(outfile, entity)
