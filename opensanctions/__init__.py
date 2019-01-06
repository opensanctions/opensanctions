import os
import json
import click
from memorious.core import manager

from opensanctions.store import iter_entities


def init():
    config_path = os.path.join(os.path.dirname(__file__), 'config')
    manager.load_path(config_path)


@click.command()
@click.option('-r', '--recon', type=click.File('w'), default='-')  # noqa
@click.option('-c', '--crawler')
def cli(recon, crawler=None):
    init()
    for entity in iter_entities(crawler=crawler):
        recon.write(json.dumps(entity))
        recon.write('\n')
