import click
from followthemoney.cli.util import write_object

from opensanctions.core import Dataset, Context, setup


@click.group(help="OpenSanctions ETL toolkit")
@click.option("-q", "--quiet", is_flag=True, default=False)
def cli(quiet=False):
    setup(quiet=quiet)


@cli.command("dump", help="Export the entities from a dataset")
@click.argument("dataset", type=click.Choice(Dataset.names()))
@click.option("-o", "--outfile", type=click.File("w"), default="-")
def dump_dataset(dataset, outfile):
    dataset = Dataset.get(dataset)
    for entity in dataset.store:
        write_object(outfile, entity)


@cli.command("crawl", help="Crawl entities into the given dataset")
@click.argument("dataset", type=click.Choice(Dataset.names()))
def crawl(dataset):
    dataset = Dataset.get(dataset)
    Context(dataset).crawl()
