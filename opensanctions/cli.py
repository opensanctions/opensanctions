import click
import logging
import asyncio
import structlog
from nomenklatura.tui import DedupeApp
from followthemoney.dedupe import Judgement
from nomenklatura.resolver import Identifier, Resolver

from opensanctions.core import Dataset, Context, setup
from opensanctions.exporters import export_metadata, export_dataset
from opensanctions.exporters.statements import export_statements, export_statements_path
from opensanctions.exporters.statements import import_statements_path
from opensanctions.exporters.common import write_object
from opensanctions.core.loader import Database
from opensanctions.core.resolver import AUTO_USER, export_pairs, get_resolver
from opensanctions.core.xref import blocking_xref
from opensanctions.core.addresses import xref_geocode
from opensanctions.core.statements import max_last_seen
from opensanctions.core.statements import resolve_all_canonical, resolve_canonical
from opensanctions.core.db import engine_tx

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


def _process(scope_name: str, crawl: bool = True, export: bool = True) -> None:
    scope = Dataset.require(scope_name)
    if crawl is True:
        for source in scope.sources:
            Context(source).crawl()

    if export is True:
        resolver = get_resolver()
        with engine_tx() as conn:
            resolve_all_canonical(conn, resolver)
        database = Database(scope, resolver, cached=True)
        database.view(scope)
        for dataset_ in scope.datasets:
            export_dataset(dataset_, database)
        export_statements()
        export_metadata()


@cli.command("crawl", help="Crawl entities into the given dataset")
@click.argument("dataset", default=Dataset.ALL, type=datasets)
def crawl(dataset):
    _process(dataset, export=False)


@cli.command("export", help="Export entities from the given dataset")
@click.argument("dataset", default=Dataset.ALL, type=datasets)
def export(dataset):
    _process(dataset, crawl=False)


@cli.command("run", help="Run the full process for the given dataset")
@click.argument("dataset", default=Dataset.ALL, type=datasets)
def run(dataset):
    _process(dataset)


@cli.command("clear", help="Delete all stored data for the given source")
@click.argument("dataset", default=Dataset.ALL, type=datasets)
def clear(dataset):
    dataset = Dataset.require(dataset)
    for source in dataset.sources:
        Context(source).clear()


@cli.command("resolve", help="Apply de-duplication to the statements table")
def resolve():
    resolver = get_resolver()
    with engine_tx() as conn:
        resolve_all_canonical(conn, resolver)


@cli.command("xref", help="Generate dedupe candidates from the given dataset")
@click.argument("dataset", default=Dataset.DEFAULT, type=datasets)
@click.option("-f", "--fuzzy", is_flag=True, type=bool, default=False)
@click.option("-l", "--limit", type=int, default=5000)
def xref(dataset, fuzzy, limit):
    dataset = Dataset.require(dataset)
    blocking_xref(dataset, limit=limit, fuzzy=fuzzy)


@cli.command("xref-geocode", help="Deduplicate addresses using geocoding")
@click.argument("dataset", default=Dataset.DEFAULT, type=datasets)
def geocode(dataset):
    dataset = Dataset.require(dataset)
    resolver = get_resolver()
    xref_geocode(dataset, resolver)


@cli.command("xref-prune", help="Remove dedupe candidates")
@click.option("-k", "--keep", type=int, default=0)
def xref_prune(keep=0):
    resolver = get_resolver()
    for edge in list(resolver.edges.values()):
        if edge.user == AUTO_USER:
            resolver.remove_edge(edge)
    resolver.prune(keep=keep)


@cli.command("dedupe", help="Interactively judge xref candidates")
@click.option("-d", "--dataset", type=datasets, default=Dataset.DEFAULT)
def dedupe(dataset):
    resolver = get_resolver()
    dataset = Dataset.require(dataset)
    db = Database(dataset, resolver)
    loader = db.view(dataset)

    def run_app() -> None:
        app = DedupeApp(
            loader=loader,
            resolver=resolver,
            title="OpenSanction De-duplication",
            log="textual.log",
        )  # type: ignore
        app.process_messages()

    asyncio.run(run_app())


@cli.command("export-pairs", help="Export pairwise judgements")
@click.argument("dataset", default=Dataset.DEFAULT, type=datasets)
@click.option("-o", "--outfile", type=click.File("w"), default="-")
def export_pairs_(dataset, outfile):
    dataset = Dataset.require(dataset)
    for obj in export_pairs(dataset):
        write_object(outfile, obj)


@cli.command("explode", help="Destroy a cluster of deduplication matches")
@click.argument("canonical_id", type=str)
def explode(canonical_id):
    resolver = get_resolver()
    resolved_id = resolver.get_canonical(canonical_id)
    with engine_tx() as conn:
        for entity_id in resolver.explode(resolved_id):
            log.info("Restore separate entity", entity=entity_id)
            resolve_canonical(conn, resolver, entity_id)
    resolver.save()


@cli.command("merge", help="Merge multiple entities as duplicates")
@click.argument("entity_ids", type=str, nargs=-1)
def merge(entity_ids):
    if len(entity_ids) < 2:
        return
    resolver = get_resolver()
    canonical_id = resolver.get_canonical(entity_ids[0])
    for other_id in entity_ids[1:]:
        other_id = Identifier.get(other_id)
        other_canonical_id = resolver.get_canonical(other_id)
        if other_canonical_id == canonical_id:
            continue
        check = resolver.check_candidate(canonical_id, other_id)
        if not check:
            log.error(
                "Cannot merge",
                canonical_id=canonical_id,
                other_id=other_id,
                edge=resolver.get_resolved_edge(canonical_id, other_id),
            )
            return
        log.info("Merge: %s -> %s" % (other_id, canonical_id))
        canonical_id = resolver.decide(canonical_id, other_id, Judgement.POSITIVE)
    resolver.save()
    log.info("Canonical: %s" % canonical_id)


@cli.command("latest", help="Show the latest data timestamp")
@click.argument("dataset", default=Dataset.DEFAULT, type=datasets)
def latest(dataset):
    ds = Dataset.require(dataset)
    with engine_tx() as conn:
        latest = max_last_seen(conn, ds)
        if latest is not None:
            print(latest.isoformat())


@cli.command("export-statements", help="Export statement data as a CSV file")
@click.argument("outfile", type=click.Path(writable=True))
def export_statements_csv(outfile):
    export_statements_path(outfile)


@cli.command("import-statements", help="Import statement data from a CSV file")
@click.argument("infile", type=click.Path(readable=True, exists=True))
def import_statements(infile):
    import_statements_path(infile)


if __name__ == "__main__":
    cli()
