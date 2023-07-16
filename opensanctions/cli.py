import sys
import click
import shutil
import logging
from typing import Optional
from zavod.logs import get_logger
from nomenklatura.tui import dedupe_ui
from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Identifier
from nomenklatura.matching import DefaultAlgorithm

from zavod.archive import dataset_path
from zavod.resolver import get_resolver
from opensanctions.core import Context, setup
from opensanctions.exporters.statements import export_statements_path
from opensanctions.exporters.statements import import_statements_path
from opensanctions.core.audit import audit_resolver
from opensanctions.core.store import get_store
from opensanctions.core.catalog import get_catalog, get_dataset_names
from opensanctions.core.training import export_training_pairs
from opensanctions.core.xref import blocking_xref
from opensanctions.core.statements import resolve_all_canonical, resolve_canonical
from opensanctions.core.db import engine_tx
from opensanctions.exporters import export, export_metadata
from opensanctions.util import write_json

log = get_logger(__name__)
datasets = click.Choice(get_dataset_names())
ALL_SCOPE = "all"
DEFAULT_SCOPE = "default"


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


@cli.command("crawl", help="Crawl entities into the given dataset")
@click.argument("dataset", default=ALL_SCOPE, type=datasets)
@click.option("-d", "--dry-run", is_flag=True, default=False)
def crawl(dataset: str, dry_run: bool):
    """Crawl all datasets within the given scope."""
    scope = get_catalog().require(dataset)
    failed = False
    for source in scope.leaves:
        ctx = Context(source, dry_run=dry_run)
        failed = failed or not ctx.crawl()
    if failed:
        sys.exit(1)


@cli.command("export", help="Export entities from the given dataset")
@click.argument("dataset", default=ALL_SCOPE, type=datasets)
@click.option("-r", "--recurse", is_flag=True, default=False)
def export_(dataset: str, recurse: bool = False):
    export(dataset, recurse=recurse)


@cli.command("export-index", help="Export global dataset index")
@click.argument("dataset", default=ALL_SCOPE, type=datasets)
def export_metadata_(dataset: str):
    dataset_ = get_catalog().require(dataset)
    export_metadata(dataset_)


@cli.command("clear", help="Delete all stored data for the given source")
@click.argument("dataset", default=ALL_SCOPE, type=datasets)
def clear(dataset: str):
    dataset_ = get_catalog().require(dataset)
    for source in dataset_.leaves:
        Context(source).clear()


@cli.command("clear-workdir", help="Delete the working path and cached source data")
@click.argument("dataset", default=ALL_SCOPE, type=datasets)
def clear_workdir(dataset: Optional[str] = None):
    ds = get_catalog().require(dataset)
    for part in ds.datasets:
        path = dataset_path(part.name)
        log.info("Clear path: %s" % path)
        shutil.rmtree(path)


@cli.command("resolve", help="Apply de-duplication to the statements table")
def resolve():
    resolver = get_resolver()
    with engine_tx() as conn:
        resolve_all_canonical(conn, resolver)


@cli.command("xref", help="Generate dedupe candidates from the given dataset")
@click.argument("dataset", default=DEFAULT_SCOPE, type=datasets)
@click.option("-l", "--limit", type=int, default=10000)
@click.option("-f", "--focus-dataset", type=str, default=None)
@click.option("-a", "--algorithm", type=str, default=DefaultAlgorithm.NAME)
@click.option("-t", "--threshold", type=float, default=0.990)
def xref(
    dataset,
    limit: int,
    threshold: float,
    algorithm: str,
    focus_dataset: Optional[str] = None,
):
    dataset = get_catalog().require(dataset)
    blocking_xref(
        dataset,
        limit=limit,
        auto_threshold=threshold,
        algorithm=algorithm,
        focus_dataset=focus_dataset,
    )


@cli.command("xref-prune", help="Remove dedupe candidates")
def xref_prune():
    resolver = get_resolver()
    resolver.prune()
    resolver.save()


@cli.command("dedupe", help="Interactively judge xref candidates")
@click.option("-d", "--dataset", type=datasets, default=ALL_SCOPE)
def dedupe(dataset):
    dataset = get_catalog().require(dataset)
    store = get_store(dataset, external=True)
    dedupe_ui(store, url_base="https://opensanctions.org/entities/%s/")


@cli.command("export-pairs", help="Export pairwise judgements")
@click.argument("dataset", default=DEFAULT_SCOPE, type=datasets)
@click.option("-o", "--outfile", type=click.File("wb"), default="-")
def export_pairs(dataset, outfile):
    dataset = get_catalog().require(dataset)
    for obj in export_training_pairs(dataset):
        write_json(obj, outfile)


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
@click.option("-f", "--force", is_flag=True, default=False)
def merge(entity_ids, force: bool = False):
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
            edge = resolver.get_resolved_edge(canonical_id, other_id)
            if force is True:
                if edge is not None:
                    log.warn("Removing existing edge", edge=edge)
                    resolver._remove_edge(edge)
            else:
                log.error(
                    "Cannot merge",
                    canonical_id=canonical_id,
                    other_id=other_id,
                    edge=edge,
                )
                return
        log.info("Merge: %s -> %s" % (other_id, canonical_id))
        canonical_id = resolver.decide(canonical_id, other_id, Judgement.POSITIVE)
    resolver.save()
    log.info("Canonical: %s" % canonical_id)


@cli.command("audit", help="Sanity-check the resolver configuration")
def audit():
    audit_resolver()


@cli.command("export-statements", help="Export statement data as a CSV file")
@click.option("-d", "--dataset", default=ALL_SCOPE, type=datasets)
@click.option("-x", "--external", is_flag=True, default=False)
@click.argument("outfile", type=click.Path(writable=True))
def export_statements_csv(outfile, dataset: str, external: bool = False):
    dataset_ = get_catalog().require(dataset)
    export_statements_path(outfile, dataset_, external=external)


@cli.command("import-statements", help="Import statement data from a CSV file")
@click.argument("infile", type=click.Path(readable=True, exists=True))
def import_statements(infile):
    import_statements_path(infile)


@cli.command("aggregate", help="Aggregate the statements for a given scope")
@click.option("-d", "--dataset", default=ALL_SCOPE, type=datasets)
@click.option("-x", "--external", is_flag=True, default=False)
def aggregate_(dataset: str, external: bool = False):
    dataset_ = get_catalog().require(dataset)
    get_store(dataset_, external=external)


@cli.command("db-pack", help="Helper function to pack DB statements")
def db_pack():
    from zavod.archive import get_archive_bucket, dataset_resource_path
    from zavod.archive import STATEMENTS_RESOURCE
    from nomenklatura.statement.serialize import PackStatementWriter
    from opensanctions.core.statements import all_statements
    from opensanctions.core.db import engine_read

    bucket = get_archive_bucket()
    for dataset in get_catalog().datasets:
        if dataset._type == "collection":
            continue
        log.info("Exporting from DB", dataset=dataset.name)
        blob_name = f"datasets/latest/{dataset.name}/{STATEMENTS_RESOURCE}"
        blob = bucket.get_blob(blob_name)
        if blob is not None:
            log.info("Exists: %r" % blob_name)
            continue

        stmt_path = dataset_resource_path(dataset, STATEMENTS_RESOURCE)
        with open(stmt_path, "wb") as fh:
            writer = PackStatementWriter(fh)
            with engine_read() as conn:
                stmts = all_statements(conn, dataset=dataset, external=True)
                for idx, stmt in enumerate(stmts):
                    if idx > 0 and idx % 50000 == 0:
                        log.info("Writing", dataset=dataset.name, idx=idx)
                    writer.write(stmt)

        blob = bucket.blob(blob_name)
        blob.upload_from_filename(stmt_path)
        log.info("Uploaded: %r" % blob_name)


if __name__ == "__main__":
    cli()
