import sys
from pathlib import Path
from typing import Optional, List, Tuple

import click
from nomenklatura.matching import DefaultAlgorithm
from nomenklatura.tui import dedupe_ui

from zavod.archive import dataset_state_path
from zavod.cli import cli, DatasetInPath, load_datasets, log
from zavod.integration import get_resolver
from zavod.integration.dedupe import blocking_xref, merge_entities, explode_cluster
from zavod.store import get_store


@cli.command("xref", help="Generate dedupe candidates from the given dataset")
@click.argument("dataset_paths", type=DatasetInPath, nargs=-1)
@click.option("-r", "--rebuild-store", is_flag=True, default=False)
@click.option("-l", "--limit", type=int, default=10000)
@click.option("-f", "--focus", type=str, multiple=True)
@click.option("-s", "--schema", type=str, default=None)
@click.option("-a", "--algorithm", type=str, default=DefaultAlgorithm.NAME)
@click.option("-t", "--threshold", type=float, default=None)
@click.option("-d", "--discount-internal", "discount_internal", type=float, default=1.0)
def xref(
    dataset_paths: List[Path],
    rebuild_store: bool,
    limit: int,
    threshold: Optional[float],
    algorithm: str,
    focus: Tuple[str, ...] = tuple(),
    schema: Optional[str] = None,
    discount_internal: float = 1.0,
) -> None:
    dataset = load_datasets(dataset_paths)
    resolver = get_resolver()
    resolver.begin()
    store = get_store(dataset, resolver)
    store.sync(clear=rebuild_store)
    blocking_xref(
        resolver,
        store,
        dataset_state_path(dataset.name),
        limit=limit,
        auto_threshold=threshold,
        algorithm=algorithm,
        focus_datasets=set(focus),
        schema_range=schema,
        discount_internal=discount_internal,
    )
    resolver.commit()


@cli.command("resolver-prune", help="Remove dedupe candidates from resolver file")
def xref_prune() -> None:
    try:
        resolver = get_resolver()
        resolver.begin()
        resolver.prune()
        resolver.commit()
    except Exception:
        log.exception("Failed to prune resolver file")
        sys.exit(1)


@cli.command("dedupe", help="Interactively decide xref candidates")
@click.argument("dataset_paths", type=DatasetInPath, nargs=-1)
@click.option("-r", "--rebuild-store", is_flag=True, default=False)
def dedupe(dataset_paths: List[Path], rebuild_store: bool = False) -> None:
    dataset = load_datasets(dataset_paths)
    resolver = get_resolver()
    resolver.begin()
    store = get_store(dataset, resolver)
    store.sync(clear=rebuild_store)
    resolver.commit()
    dedupe_ui(resolver, store, url_base="https://opensanctions.org/entities/%s/")


@cli.command("explode-cluster", help="Destroy a cluster of deduplication matches")
@click.argument("canonical_id", type=str)
def explode(canonical_id: str) -> None:
    resolver = get_resolver()
    resolver.begin()
    explode_cluster(resolver, canonical_id)
    resolver.commit()


@cli.command("merge-cluster", help="Merge multiple entities as duplicates")
@click.argument("entity_ids", type=str, nargs=-1)
@click.option("-f", "--force", is_flag=True, default=False)
def merge(entity_ids: List[str], force: bool = False) -> None:
    try:
        resolver = get_resolver()
        resolver.begin()
        merge_entities(resolver, entity_ids, force=force)
        resolver.commit()
    except ValueError as ve:
        log.error("Cannot merge: %s" % ve)
        sys.exit(1)


@cli.command("dedupe-edges", help="Merge edge entities that are effectively duplicates")
@click.argument("dataset_paths", type=DatasetInPath, nargs=-1)
@click.option("-r", "--rebuild-store", is_flag=True, default=False)
def dedupe_edges(dataset_paths: List[Path], rebuild_store: bool = False) -> None:
    from zavod.integration import edges

    dataset = load_datasets(dataset_paths)
    resolver = get_resolver()
    try:
        resolver.begin()
        store = get_store(dataset, resolver)
        store.sync(clear=rebuild_store)
        edges.dedupe_edges(resolver, store.view(dataset, external=True))
        resolver.commit()
    except Exception:
        resolver.rollback()
        log.exception("Failed to dedupe edge entities: %r" % dataset_paths)
        sys.exit(1)
