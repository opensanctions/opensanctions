import orjson
import logging
import click
from followthemoney import StatementEntity
from pathlib import Path
from sqlalchemy import create_engine, MetaData
from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Resolver, Linker, Edge
from nomenklatura.resolver.identifier import Identifier
from typing import Any, Optional, Tuple, Generator

from zavod.entity import Entity
from zavod import settings
from zavod.integration.dedupe import get_resolver
from zavod.logs import get_logger, configure_logging
from zavod.meta import get_catalog, get_multi_dataset
from zavod.meta.dataset import Dataset
from zavod.store import View, get_store

log = get_logger(Path(__file__).stem)

InFile = click.Path(dir_okay=False, readable=True, file_okay=True, path_type=Path)
OutFile = click.Path(dir_okay=False, writable=True, file_okay=True, path_type=Path)

# These datasets have particularly jiggy data quality, so we don't want to derive training data
# from judgements linked to them.
IGNORE_DATASETS = {
    "us_sam_exclusions",  # horrific data quality
    "us_fed_enforcements",  # too much name-only matching
    "us_ddtc_debarred",  # too much name-only matching
    "us_cia_world_factbook",  # too much name-only matching
    "un_ga_protocol",  # too much name-only matching
    "tw_shtc",  # too much name-only matching
    "opencorporates",  # funky enricher
    "us_ofac_press_releases",  # too much name-only matching
    "ext_us_ofac_press_releases",  # too much name-only matching
    "eu_esma_sanctions",  # too much name-only matching
}


def make_replay_resolver() -> Resolver[StatementEntity]:
    engine = create_engine("sqlite:///:memory:")
    meta = MetaData()
    meta.reflect(engine)
    return Resolver[StatementEntity](engine, meta, create=True)


def replay_edges() -> Generator[Edge, None, None]:
    resolver = get_resolver()
    resolver.begin()
    edges = [edge for edge in resolver.edges.values()]
    edges = [e for e in edges if e.deleted_at is None]
    edges = [e for e in edges if e.judgement != Judgement.NO_JUDGEMENT]
    edges.sort(key=lambda e: e.created_at or "XXXX")
    # resolver.rollback()
    log.info(f"Replaying {len(edges)} edges...")
    for edge in edges:
        yield edge


def lazy_resolve(view: View, resolver: Resolver, id: Identifier) -> Optional[Entity]:
    """Get an entity merging all connected entities from the view."""
    cluster: Optional[Entity] = None
    connected = resolver.connected(id)
    cluster_id = max(connected).id

    for ident in connected:
        entity = view.get_entity(ident.id)
        if entity is None or entity.schema.is_a("Address"):
            continue
        if cluster is None:
            cluster = entity
        else:
            try:
                cluster.merge(entity)
            except Exception as e:
                log.warning(
                    f"Error merging entities: {e}",
                    cluster=cluster_id,
                    id1=cluster.id,
                    id2=entity.id,
                )
                return None
    if cluster is not None:
        cluster.id = cluster_id
    return cluster


def generate_pairs(
    dataset: Dataset,
) -> Generator[Tuple[Entity, Entity, Judgement], None, None]:
    store = get_store(dataset, Linker({}))
    store.sync()
    view = store.view(dataset, external=True)

    something_missing_count = 0
    positive_count = 0
    negative_count = 0

    resolver = make_replay_resolver()
    resolver.begin()

    for idx, edge in enumerate(replay_edges()):
        judgement = edge.judgement
        if judgement == Judgement.UNSURE:
            judgement = Judgement.NEGATIVE

        if idx % 10000 == 0 and idx > 0:
            log.info(
                f"Processed {idx} edges...",
                positive=positive_count,
                negative=negative_count,
                missing=something_missing_count,
            )

        source = lazy_resolve(view, resolver, edge.source)
        target = lazy_resolve(view, resolver, edge.target)

        # Register after loading to get merged entity next time it's referenced.
        resolver._register(edge)
        resolver._invalidate()

        missing = []
        if source is None:
            missing.append(edge.source.id)
        if target is None:
            missing.append(edge.target.id)
        if source is None or target is None:
            something_missing_count += 1
            log.debug("Couldn't find source and or target in store.", missing=missing)
            continue

        if judgement == Judgement.POSITIVE:
            positive_count += 1
        else:
            negative_count += 1

        yield source, target, judgement

    log.info(
        f"Positive: {positive_count}, Negative: {negative_count}, Missing: {something_missing_count}"
    )


# def generate_groups(
#     dataset: Dataset,
# ) -> Generator[Dict[str, Entity | str | int], None, None]:
#     """
#     Each list of pairs is a disconnected subgraph.
#     Negative judgements are considered edges too.
#     """
#     log.info("Generating pairs...")
#     g = nx.Graph()
#     for i, (source, target, judgement) in enumerate(generate_pairs(dataset)):
#         if i % 10000 == 0 and i > 0:
#             log.info(f"Generated {i} pairs...")

#         source_canonical = complete_resolver.get_canonical(source.id)
#         target_canonical = complete_resolver.get_canonical(target.id)
#         # add to graph
#         g.add_node(source_canonical)
#         g.add_node(target_canonical)
#         g.add_edge(
#             source_canonical,
#             target_canonical,
#             source=source,
#             target=target,
#             judgement=judgement,
#         )

#     log.info("Generating disjoint subgraphs...")
#     for ix, subgraph_nodes in enumerate(nx.connected_components(g)):
#         for _, _, data in g.subgraph(subgraph_nodes).copy().edges(data=True):
#             yield {
#                 "left": data["source"].to_dict(),
#                 "right": data["target"].to_dict(),
#                 "judgement": data["judgement"].value,
#                 "group": ix,
#             }


def generate_naive(dataset: Dataset) -> Generator[Any, None, None]:
    """
    Each pair of entities from the same cluster is a positive example.
    Random pairs of entities from different clusters are negative examples.
    """
    for i, (source, target, judgement) in enumerate(generate_pairs(dataset)):
        if i % 10000 == 0 and i > 0:
            log.info(f"Generated {i} pairs...")
        yield {
            "left": source.to_dict(),
            "right": target.to_dict(),
            "judgement": judgement.value,
        }


@click.command()
@click.argument("scope", type=str, default="default")
@click.argument("outfile", type=OutFile, default=settings.DATA_PATH / "pairs.json")
@click.option("--log-level", default="INFO")
def main(scope: str, outfile: Path, log_level: str):
    configure_logging(level=logging.getLevelNamesMapping()[log_level])
    dataset_scope = get_catalog().require(scope)
    datasets = [d for d in dataset_scope.datasets if d.name not in IGNORE_DATASETS]
    datasets = [d for d in datasets if not d.is_collection]
    dataset = get_multi_dataset([d.name for d in datasets])

    with open(outfile, "wb") as out_fh:
        for pair in generate_naive(dataset):
            out_fh.write(orjson.dumps(pair, option=orjson.OPT_APPEND_NEWLINE))


if __name__ == "__main__":
    main()
