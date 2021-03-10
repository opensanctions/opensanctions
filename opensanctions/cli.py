import click
import logging
from followthemoney.cli.util import write_object

from opensanctions.core import Dataset, Context, setup


@click.group(help="OpenSanctions ETL toolkit")
@click.option("-v", "--verbose", is_flag=True, default=False)
@click.option("-q", "--quiet", is_flag=True, default=False)
def cli(verbose=False, quiet=False):
    level = logging.INFO
    if quiet:
        level = logging.ERROR
    if verbose:
        level = logging.DEBUG
    setup(log_level=level)


@cli.command("dump", help="Export the entities from a dataset")
@click.argument("dataset", default=Dataset.ALL, type=click.Choice(Dataset.names()))
@click.option("-o", "--outfile", type=click.File("w"), default="-")
def dump_dataset(dataset, outfile):
    dataset = Dataset.get(dataset)
    for source in dataset.sources:
        # TODO: consolidate the data
        for entity in source.store:
            write_object(outfile, entity)


@cli.command("crawl", help="Crawl entities into the given dataset")
@click.argument("dataset", default=Dataset.ALL, type=click.Choice(Dataset.names()))
def crawl(dataset):
    dataset = Dataset.get(dataset)
    for source in dataset.sources:
        Context(source).crawl()
