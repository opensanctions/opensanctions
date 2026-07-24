import sys
from pathlib import Path

import click
from followthemoney import Dataset as FTMDataset
from nomenklatura.cache import Cache
from nomenklatura.db import make_session
from nomenklatura.matching import DefaultAlgorithm, DedupeAlgorithm, get_algorithm
from nomenklatura.tui import dedupe_ui, reconcile_ui
from nomenklatura.wikidata.client import WikidataClient
from nomenklatura.wikidata.write import serialize

from zavod.archive import dataset_state_path
from zavod.cli import cli, DatasetInPath, _load_datasets, log
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
    dataset_paths: list[Path],
    rebuild_store: bool,
    limit: int,
    threshold: float | None,
    algorithm: str,
    focus: tuple[str, ...] = tuple(),
    schema: str | None = None,
    discount_internal: float = 1.0,
) -> None:
    dataset = _load_datasets(dataset_paths)
    with make_session() as session:
        resolver = get_resolver(session)
        resolver.load_into_memory()
        store = get_store(dataset, resolver)
        store.sync(clear=rebuild_store)
        blocking_xref(
            resolver,
            session,
            store,
            dataset_state_path(dataset.name),
            limit=limit,
            auto_threshold=threshold,
            algorithm=algorithm,
            focus_datasets=set(focus),
            schema_range=schema,
            discount_internal=discount_internal,
        )


@cli.command("resolver-prune", help="Remove dedupe candidates from resolver file")
def xref_prune() -> None:
    try:
        with make_session() as session:
            resolver = get_resolver(session)
            resolver.load_into_memory()
            resolver.prune()
    except Exception:
        log.exception("Failed to prune resolver file")
        sys.exit(1)


@cli.command("dedupe", help="Interactively decide xref candidates")
@click.argument("dataset_paths", type=DatasetInPath, nargs=-1)
@click.option("-r", "--rebuild-store", is_flag=True, default=False)
def dedupe(dataset_paths: list[Path], rebuild_store: bool = False) -> None:
    dataset = _load_datasets(dataset_paths)
    with make_session() as session:
        resolver = get_resolver(session)
        resolver.load_into_memory()
        store = get_store(dataset, resolver)
        store.sync(clear=rebuild_store)
        dedupe_ui(
            resolver, session, store, url_base="https://opensanctions.org/entities/%s/"
        )


@cli.command(
    "wikidata-reconcile",
    help="Match dataset persons against Wikidata in a review UI",
)
@click.argument("dataset_paths", type=DatasetInPath, nargs=-1)
@click.option("-r", "--rebuild-store", is_flag=True, default=False)
@click.option("--aliases/--no-aliases", default=True)
@click.option("-a", "--algorithm", type=str, default=DedupeAlgorithm.NAME)
@click.option(
    "-o",
    "--output",
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    default=None,
    help="QuickStatements output path (default: <dataset state>/wikidata.qs)",
)
def wikidata_reconcile(
    dataset_paths: list[Path],
    rebuild_store: bool = False,
    aliases: bool = True,
    algorithm: str = DedupeAlgorithm.NAME,
    output: Path | None = None,
) -> None:
    """Interactively reconcile dataset persons against Wikidata.

    Each Person is presented against its ranked Wikidata search candidates for a
    human decision (confirm / no-match / unsure / create / skip). Confirmed
    matches are written to the resolver, and the run emits a QuickStatements
    batch the operator runs in the QS web UI to enrich the matched items and
    create new ones for unmatched persons. The dataset's own `url` and
    `updated_at` are used as the fallback source and retrieved-on citation for
    statements whose entity lacks a `sourceUrl` of its own.
    """
    dataset = _load_datasets(dataset_paths)
    algorithm_type = get_algorithm(algorithm)
    if algorithm_type is None:
        raise click.UsageError(f"Unknown algorithm: {algorithm}")
    if output is None:
        output = dataset_state_path(dataset.name) / "wikidata.qs"

    session = make_session()
    resolver = get_resolver(session)
    resolver.load_into_memory()
    store = get_store(dataset, resolver)
    store.sync(clear=rebuild_store)

    # Cite the dataset itself when an entity carries no sourceUrl/retrieved date.
    retrieved: str | None = None
    if dataset.model.updated_at is not None:
        retrieved = dataset.model.updated_at.date().isoformat()

    # A throwaway FtM dataset namespaces the shared Wikidata API cache and the
    # candidate-entity projection built by the reconciler.
    wikidata = FTMDataset.make({"name": "wikidata", "title": "Wikidata"})
    cache = Cache(session, wikidata, create=True)
    client = WikidataClient(cache)
    try:
        # reconcile_ui checkpoints the session per judgement, so we don't hold a
        # single transaction open across the UI.
        commands = reconcile_ui(
            resolver,
            session,
            store,
            client,
            wikidata,
            algorithm_type,
            aliases=aliases,
            retrieved=retrieved,
            source_url=dataset.url,
            url_base="https://opensanctions.org/entities/%s/",
        )
    finally:
        # Persist cached API responses and resolver judgements even if the run is
        # cancelled or errors. A plain commit (not a `with`) so a mid-run failure
        # keeps the cache writes rather than rolling them back.
        session.commit()

    output.parent.mkdir(parents=True, exist_ok=True)
    text = serialize(commands)
    if len(text):
        text += "\n"
    output.write_text(text)
    log.info("Wrote %d QuickStatements commands: %s" % (len(commands), output))


@cli.command("explode-cluster", help="Destroy a cluster of deduplication matches")
@click.argument("canonical_id", type=str)
def explode(canonical_id: str) -> None:
    with make_session() as session:
        resolver = get_resolver(session)
        resolver.load_into_memory()
        explode_cluster(resolver, canonical_id)


@cli.command("merge-cluster", help="Merge multiple entities as duplicates")
@click.argument("entity_ids", type=str, nargs=-1)
@click.option("-f", "--force", is_flag=True, default=False)
def merge(entity_ids: list[str], force: bool = False) -> None:
    try:
        with make_session() as session:
            resolver = get_resolver(session)
            resolver.load_into_memory()
            merge_entities(resolver, entity_ids, force=force)
    except ValueError as ve:
        log.error(f"Cannot merge: {ve}")
        sys.exit(1)


@cli.command("dedupe-edges", help="Merge edge entities that are effectively duplicates")
@click.argument("dataset_paths", type=DatasetInPath, nargs=-1)
@click.option("-r", "--rebuild-store", is_flag=True, default=False)
def dedupe_edges(dataset_paths: list[Path], rebuild_store: bool = False) -> None:
    from zavod.integration import edges

    dataset = _load_datasets(dataset_paths)
    try:
        with make_session() as session:
            resolver = get_resolver(session)
            resolver.load_into_memory()
            store = get_store(dataset, resolver)
            store.sync(clear=rebuild_store)
            edges.dedupe_edges(resolver, session, store.view(dataset, external=True))
    except Exception:
        log.exception(f"Failed to dedupe edge entities: {dataset_paths!r}")
        sys.exit(1)
