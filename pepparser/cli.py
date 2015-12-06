import click
import logging

from pepparser.parsers.ofac import ofac_parse


@click.group()
@click.option('--debug/--no-debug', default=False)
def cli(debug):
    fmt = '[%(levelname)-8s] %(name)-12s: %(message)s'
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format=fmt)
    logging.getLogger('requests').setLevel(logging.WARNING)


@cli.command()
@click.option('--sdn', default=False, is_flag=True)
@click.option('--consolidated', default=False, is_flag=True)
@click.argument('xmlfile')
def ofac(sdn, consolidated, xmlfile):
    ofac_parse(sdn, consolidated, xmlfile)


if __name__ == '__main__':
    cli()
