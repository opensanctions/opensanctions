import click
import logging
import asyncio
import structlog
from nomenklatura.tui import DedupeApp
from nomenklatura.util import coro
from followthemoney.dedupe import Judgement
from nomenklatura.resolver import Identifier, Resolver

from opensanctions.core import Dataset, Context, setup
from opensanctions.exporters import export_metadata, export_dataset
from opensanctions.exporters import export_statements
from opensanctions.exporters.common import write_object
from opensanctions.core.http import cleanup_cache
from opensanctions.core.loader import Database
from opensanctions.core.resolver import AUTO_USER, export_pairs, get_resolver
from opensanctions.core.xref import blocking_xref
from opensanctions.core.addresses import xref_geocode
from opensanctions.core.statements import (
    max_last_seen,
    resolve_all_canonical,
    resolve_canonical,
)
from opensanctions.core.db import migrate_db, engine

log = structlog.get_logger(__name__)
datasets = click.Choice(Dataset.names())


@click.group(help="OpenSanctions ETL toolkit")
@click.option("-v", "--verbose", is_flag=True, default=False)
@click.option("-q", "--quiet", is_flag=True, default=False)
def cli(verbose=False, quiet=False):
    level = logging.INFO
    if quiet:
        level = logging.WARNING
    if verbose:
        level = logging.DEBUG
    setup(log_level=level)


async def _resolve_all(resolver: Resolver):
    async with engine.begin() as conn:
        await resolve_all_canonical(conn, resolver)


async def _process(scope_name: str, crawl: bool = True, export: bool = True) -> None:
    scope = Dataset.require(scope_name)
    if crawl is True:
        crawls = []
        for source in scope.sources:
            crawls.append(Context(source).crawl())
        await asyncio.gather(*crawls)
    if export is True:
        resolver = await get_resolver()
        await _resolve_all(resolver)
        database = Database(scope, resolver, cached=True)
        await database.view(scope)
        # exports = []
        # for dataset_ in scope.datasets:
        #     exports.append(export_dataset(dataset_, database))
        # await asyncio.gather(*exports)
        for dataset_ in scope.datasets:
            await export_dataset(dataset_, database)
        await export_metadata()
        await export_statements()


@cli.command("crawl", help="Crawl entities into the given dataset")
@click.argument("dataset", default=Dataset.ALL, type=datasets)
@coro
async def crawl(dataset):
    await _process(dataset, export=False)


@cli.command("export", help="Export entities from the given dataset")
@click.argument("dataset", default=Dataset.ALL, type=datasets)
@coro
async def export(dataset):
    await _process(dataset, crawl=False)


@cli.command("run", help="Run the full process for the given dataset")
@click.argument("dataset", default=Dataset.ALL, type=datasets)
@coro
async def run(dataset):
    await _process(dataset)


@cli.command("clear", help="Delete all stored data for the given source")
@click.argument("dataset", default=Dataset.ALL, type=datasets)
@coro
async def clear(dataset):
    dataset = Dataset.require(dataset)
    for source in dataset.sources:
        Context(source).clear()


@cli.command("resolve", help="Apply de-duplication to the statements table")
@coro
async def resolve():
    resolver = await get_resolver()
    await _resolve_all(resolver)


@cli.command("xref", help="Generate dedupe candidates from the given dataset")
@click.argument("dataset", default=Dataset.DEFAULT, type=datasets)
@click.option("-f", "--fuzzy", is_flag=True, type=bool, default=False)
@click.option("-l", "--limit", type=int, default=5000)
@coro
async def xref(dataset, fuzzy, limit):
    dataset = Dataset.require(dataset)
    await blocking_xref(dataset, limit=limit, fuzzy=fuzzy)


@cli.command("xref-geocode", help="Deduplicate addresses using geocoding")
@click.argument("dataset", default=Dataset.DEFAULT, type=datasets)
@coro
async def geocode(dataset):
    dataset = Dataset.require(dataset)
    await xref_geocode(dataset)


@cli.command("xref-prune", help="Remove dedupe candidates")
@click.option("-k", "--keep", type=int, default=0)
@coro
async def xref_prune(keep=0):
    resolver = await get_resolver()
    for edge in list(resolver.edges.values()):
        if edge.user == AUTO_USER:
            resolver.remove_edge(edge)
    resolver.prune(keep=keep)
    await resolver.save()


@cli.command("dedupe", help="Interactively judge xref candidates")
@click.option("-d", "--dataset", type=datasets, default=Dataset.DEFAULT)
@coro
async def dedupe(dataset):
    resolver = await get_resolver()
    dataset = Dataset.require(dataset)
    db = Database(dataset, resolver)
    loader = await db.view(dataset)
    app = DedupeApp(
        loader=loader,
        resolver=resolver,
        title="OpenSanction De-duplication",
        log="textual.log",
    )  # type: ignore
    await app.process_messages()


@cli.command("export-pairs", help="Export pairwise judgements")
@click.argument("dataset", default=Dataset.DEFAULT, type=datasets)
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@coro
async def export_pairs_(dataset, outfile):
    dataset = Dataset.require(dataset)
    async for obj in export_pairs(dataset):
        write_object(outfile, obj)


@cli.command("explode", help="Destroy a cluster of deduplication matches")
@click.argument("canonical_id", type=str)
@coro
async def explode(canonical_id):
    resolver = await get_resolver()
    resolved_id = resolver.get_canonical(canonical_id)
    async with engine.begin() as conn:
        for entity_id in resolver.explode(resolved_id):
            log.info("Restore separate entity", entity=entity_id)
            await resolve_canonical(conn, resolver, entity_id)
    await resolver.save()


@cli.command("merge", help="Merge multiple entities as duplicates")
@click.argument("entity_ids", type=str, nargs=-1)
@coro
async def merge(entity_ids):
    if len(entity_ids) < 2:
        return
    resolver = await get_resolver()
    canonical_id = resolver.get_canonical(entity_ids[0])
    for other_id in entity_ids[1:]:
        other_id = Identifier.get(other_id)
        other_canonical_id = resolver.get_canonical(other_id)
        if other_canonical_id == canonical_id:
            continue
        check = await resolver.check_candidate(canonical_id, other_id)
        if not check:
            log.error(
                "Cannot merge",
                canonical_id=canonical_id,
                other_id=other_id,
                edge=resolver.get_resolved_edge(canonical_id, other_id),
            )
            return
        log.info("Merge: %s -> %s" % (other_id, canonical_id))
        canonical_id = await resolver.decide(canonical_id, other_id, Judgement.POSITIVE)
    await resolver.save()
    log.info("Canonical: %s" % canonical_id)


@cli.command("latest", help="Show the latest data timestamp")
@click.argument("dataset", default=Dataset.DEFAULT, type=datasets)
@coro
async def latest(dataset):
    ds = Dataset.require(dataset)
    async with engine.begin() as conn:
        latest = await max_last_seen(conn, ds)
        if latest is not None:
            print(latest.isoformat())


@cli.command("cleanup", help="Clean up caches")
@coro
async def cleanup():
    cleanup_cache()


@cli.command("migrate", help="Create a new database autogenerated migration")
@click.option("-m", "--message", "message")
def migrate(message):
    migrate_db(message)


if __name__ == "__main__":
    cli()
