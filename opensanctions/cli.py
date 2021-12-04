import click
import logging
from nomenklatura.resolver import Identifier
import structlog
from nomenklatura.tui import DedupeApp
from followthemoney.dedupe import Judgement

from opensanctions.core import Dataset, Context, setup
from opensanctions.exporters import export_global_index, export_dataset
from opensanctions.exporters.common import write_object
from opensanctions.core.http import cleanup_cache
from opensanctions.core.index import get_index, get_index_path
from opensanctions.core.loader import Database
from opensanctions.core.resolver import AUTO_USER, export_pairs, get_resolver
from opensanctions.core.xref import blocking_xref
from opensanctions.model.statement import Statement
from opensanctions.model.base import migrate_db

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


@cli.command("dump", help="Export the entities from a dataset")
@click.argument("dataset", default=Dataset.ALL, type=datasets)
@click.option("-o", "--outfile", type=click.File("w"), default="-")
def dump_dataset(dataset, outfile):
    dataset = Dataset.require(dataset)
    resolver = get_resolver()
    loader = Database(dataset, resolver).view(dataset)
    for entity in loader:
        write_object(outfile, entity)


@cli.command("crawl", help="Crawl entities into the given dataset")
@click.argument("dataset", default=Dataset.ALL, type=datasets)
def crawl(dataset):
    dataset = Dataset.require(dataset)
    for source in dataset.sources:
        Context(source).crawl()


@cli.command("export", help="Export entities from the given dataset")
@click.argument("dataset", default=Dataset.ALL, type=datasets)
def export(dataset):
    resolver = get_resolver()
    Statement.resolve_all(resolver)
    dataset = Dataset.require(dataset)
    database = Database(dataset, resolver, cached=True)
    for dataset_ in dataset.datasets:
        export_dataset(dataset_, database)
    export_global_index()


@cli.command("run", help="Run the full process for the given dataset")
@click.argument("dataset", default=Dataset.ALL, type=datasets)
def run(dataset):
    dataset = Dataset.require(dataset)
    resolver = get_resolver()
    for source in dataset.sources:
        Context(source).crawl()
    Statement.resolve_all(resolver)
    database = Database(dataset, resolver, cached=True)
    for dataset_ in dataset.datasets:
        export_dataset(dataset_, database)
    export_global_index()


@cli.command("clear", help="Delete all stored data for the given source")
@click.argument("dataset", default=Dataset.ALL, type=datasets)
def clear(dataset):
    dataset = Dataset.require(dataset)
    for source in dataset.sources:
        Context(source).clear()


@cli.command("resolve", help="Apply de-duplication to the statements table")
def resolve():
    resolver = get_resolver()
    Statement.resolve_all(resolver)


@cli.command("index", help="Index entities from the given dataset")
@click.argument("dataset", default=Dataset.DEFAULT, type=datasets)
def index(dataset):
    resolver = get_resolver()
    # Statement.resolve_all(resolver)
    dataset = Dataset.require(dataset)
    database = Database(dataset, resolver, cached=True)
    loader = database.view(dataset)
    path = get_index_path(dataset)
    path.unlink(missing_ok=True)
    get_index(dataset, loader)


@cli.command("xref", help="Generate dedupe candidates from the given dataset")
@click.argument("dataset", default=Dataset.DEFAULT, type=datasets)
@click.option("-f", "--fuzzy", is_flag=True, type=bool, default=False)
@click.option("-l", "--limit", type=int, default=5000)
def xref(dataset, fuzzy, limit):
    dataset = Dataset.require(dataset)
    blocking_xref(dataset, limit=limit, fuzzy=fuzzy)


@cli.command("xref-prune", help="Remove dedupe candidates")
@click.option("-k", "--keep", type=int, default=0)
def xref_prune(keep=0):
    resolver = get_resolver()
    for edge in list(resolver.edges.values()):
        if edge.user == AUTO_USER:
            resolver._remove(edge)
    resolver.prune(keep=keep)
    resolver.save()


@cli.command("dedupe", help="Interactively judge xref candidates")
@click.option("-d", "--dataset", type=datasets, default=Dataset.DEFAULT)
def dedupe(dataset):
    resolver = get_resolver()
    dataset = Dataset.require(dataset)
    db = Database(dataset, resolver)
    DedupeApp.run(
        title="OpenSanction De-duplication",
        # log="textual.log",
        loader=db.view(dataset),
        resolver=resolver,
    )


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
    resolver.explode(resolved_id)
    resolver.save()
    Statement.resolve_all(resolver)


@cli.command("merge", help="Merge multiple entities as duplicates")
@click.argument("entity_ids", type=str, nargs=-1)
def merge(entity_ids):
    if len(entity_ids) < 2:
        return
    resolver = get_resolver()
    canonical_id = resolver.get_canonical(entity_ids[0])
    for other_id in entity_ids[1:]:
        other_id = Identifier.get(other_id)
        if other_id == canonical_id:
            continue
        if not resolver.check_candidate(canonical_id, other_id):
            log.error("Cannot merge", canonical_id=canonical_id, other_id=other_id)
            return
        log.info("Merge: %s -> %s" % (other_id, canonical_id))
        canonical_id = resolver.decide(canonical_id, other_id, Judgement.POSITIVE)
    resolver.save()
    log.info("Canonical: %s" % canonical_id)
    Statement.resolve(resolver, str(canonical_id))


@cli.command("cleanup", help="Clean up caches")
def cleanup():
    cleanup_cache()


@cli.command("migrate", help="Create a new database autogenerated migration")
@click.option("-m", "--message", "message")
def migrate(message):
    migrate_db(message)


if __name__ == "__main__":
    cli()
