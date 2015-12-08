import click
import logging
import dataset

from pepparser.emitter import Emitter
from pepparser.parsers.ofac import ofac_parse
from pepparser.parsers.eeas import eeas_parse
from pepparser.parsers.sdfm import sdfm_parse
from pepparser.parsers.unsc import unsc_parse
from pepparser.parsers.seco import seco_parse
from pepparser.parsers.hmt import hmt_parse
from pepparser.parsers.usbis import usbis_parse
from pepparser.parsers.wbdeb import wbdeb_parse
from pepparser.parsers.interpol import interpol_parse
from pepparser.parsers.cia_world_leaders import worldleaders_parse
from pepparser.parsers.every_politician import everypolitician_parse

log = logging.getLogger(__name__)


@click.group()
@click.option('--debug/--no-debug', default=False)
@click.option('database_uri', '--db', envvar='DATABASE_URI', required=True)
@click.pass_context
def cli(ctx, debug, database_uri):
    fmt = '[%(levelname)-8s] %(name)-12s: %(message)s'
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format=fmt)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('alembic').setLevel(logging.WARNING)

    ctx.obj['debug'] = debug
    ctx.obj['database_uri'] = database_uri
    log.debug('Connecting to DB: %r', database_uri)
    ctx.obj['engine'] = dataset.connect(database_uri)


@cli.group()
@click.pass_context
def parse(ctx):
    ctx.obj['emit'] = Emitter(ctx.obj['engine'])


@parse.command()
@click.option('--sdn', default=False, is_flag=True)
@click.option('--consolidated', default=False, is_flag=True)
@click.argument('xmlfile')
@click.pass_context
def ofac(ctx, sdn, consolidated, xmlfile):
    ofac_parse(ctx.obj['emit'], sdn, consolidated, xmlfile)
    ctx.obj['emit'].save()


@parse.command()
@click.argument('xmlfile')
@click.pass_context
def sdfm(ctx, xmlfile):
    sdfm_parse(ctx.obj['emit'], xmlfile)
    ctx.obj['emit'].save()


@parse.command()
@click.argument('xmlfile')
@click.pass_context
def eeas(ctx, xmlfile):
    eeas_parse(ctx.obj['emit'], xmlfile)
    ctx.obj['emit'].save()


@parse.command()
@click.argument('xmlfile')
@click.pass_context
def unsc(ctx, xmlfile):
    unsc_parse(ctx.obj['emit'], xmlfile)
    ctx.obj['emit'].save()


@parse.command()
@click.argument('xmlfile')
@click.pass_context
def seco(ctx, xmlfile):
    seco_parse(ctx.obj['emit'], xmlfile)
    ctx.obj['emit'].save()


@parse.command()
@click.argument('csvfile')
@click.pass_context
def hmt(ctx, csvfile):
    hmt_parse(ctx.obj['emit'], csvfile)
    ctx.obj['emit'].save()


@parse.command()
@click.argument('csvfile')
@click.pass_context
def usbis(ctx, csvfile):
    usbis_parse(ctx.obj['emit'], csvfile)
    ctx.obj['emit'].save()


@parse.command()
@click.argument('jsonfile')
@click.pass_context
def worldleaders(ctx, jsonfile):
    worldleaders_parse(ctx.obj['emit'], jsonfile)
    ctx.obj['emit'].save()


@parse.command()
@click.argument('jsonfile')
@click.pass_context
def interpol(ctx, jsonfile):
    interpol_parse(ctx.obj['emit'], jsonfile)
    ctx.obj['emit'].save()


@parse.command()
@click.argument('htmlfile')
@click.pass_context
def wbdeb(ctx, htmlfile):
    wbdeb_parse(ctx.obj['emit'], htmlfile)
    ctx.obj['emit'].save()


@parse.command()
@click.argument('jsonfile')
@click.pass_context
def everypolitician(ctx, jsonfile):
    everypolitician_parse(ctx.obj['emit'], jsonfile)
    ctx.obj['emit'].save()


def main():
    cli(obj={})

if __name__ == '__main__':
    main()
