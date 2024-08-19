from collections import defaultdict
from random import shuffle
from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Resolver, Linker, Edge
from nomenklatura.matching.pairs import JudgedPair
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Generator
import click
import json
import networkx as nx


from zavod import settings
from zavod.entity import Entity
from zavod.logs import get_logger, configure_logging
from zavod.meta import load_dataset_from_path
from zavod.meta.dataset import Dataset
from zavod.store import View, get_store

log = get_logger(__name__)

InFile = click.Path(dir_okay=False, readable=True, file_okay=True, path_type=Path)
OutFile = click.Path(dir_okay=False, writable=True, file_okay=True, path_type=Path)


class ChronAggResolver(Resolver):
    def chrono_edges(self, path: Path) -> Generator[Edge, None, None]:
        edges = []
        with open(path, "r") as fh:
            while True:
                line = fh.readline()
                if not line:
                    break
                edge = Edge.from_line(line)
                edges.append(edge)
        edges.sort(key=lambda e: e.timestamp)
        yield from edges


def resolve(view: View, resolver: Resolver, ids: Set[str]) -> Optional[Entity]:
    cluster: Optional[Entity] = None
    for ident in ids:
        entity = view.get_entity(ident.id)
        if entity is None:
            continue
        if cluster is None:
            cluster = entity
        else:
            cluster.merge(entity)
    return cluster


def generate_pairs(
    resolver_path: Path,
    dataset: Dataset,
) -> Generator[Tuple[Entity, Entity, Judgement], None, None]:
    resolver = ChronAggResolver(resolver_path)
    store = get_store(dataset, Linker({}))
    store.sync()
    view = store.view(dataset, external=False)

    something_missing_count = 0
    positive_count = 0
    negative_count = 0

    for edge in resolver.chrono_edges(resolver.path):
        if edge.judgement == Judgement.NO_JUDGEMENT:
            continue

        judgement = edge.judgement
        if judgement == Judgement.UNSURE:
            judgement = Judgement.NEGATIVE

        # TODO: Should this be using their canonical IDs?
        source_cluster_ids = resolver.connected(edge.source)
        target_cluster_ids = resolver.connected(edge.target)

        resolver._register(edge)
        resolver.connected.cache_clear()

        # Lazily resolve
        source = resolve(view, resolver, source_cluster_ids)
        target = resolve(view, resolver, target_cluster_ids)
        if source is None or target is None:
            something_missing_count += 1
            continue

        if judgement == Judgement.POSITIVE:
            positive_count += 1
        else:
            negative_count += 1

        yield source, target, judgement

    log.info(
        f"Positive: {positive_count}, Negative: {negative_count}, Missing: {something_missing_count}"
    )


def generate_groups(
    resolver_path: Path, dataset: Dataset
) -> Generator[List[Dict[str, Entity | str]], None, None]:
    """
    Each list of pairs is a disconnected subgraph.
    Negative judgements are considered edges too.
    """

    log.info("Loading resolver...")
    resolver = Resolver.load(resolver_path)

    log.info("Generating pairs...")
    g = nx.Graph()
    for i, (source, target, judgement) in enumerate(generate_pairs(resolver_path, dataset)):
        if i % 10000 == 0 and i > 0:
            log.info(f"Generated {i} pairs...")

        source_canonical = resolver.get_canonical(source.id)
        target_canonical = resolver.get_canonical(target.id)
        # add to graph
        g.add_node(source_canonical)
        g.add_node(target_canonical)
        g.add_edge(
            source_canonical,
            target_canonical,
            source=source,
            target=target,
            judgement=judgement,
        )

    log.info("Generating disjoint subgraphs...")
    for ix, subgraph_nodes in enumerate(nx.connected_components(g)):
        for _, _, data in (
            g.subgraph(subgraph_nodes)
            .copy()
            .edges(data=True)
        ):
            yield {
                "left": data["source"].to_dict(),
                "right": data["target"].to_dict(),
                "judgement": data["judgement"].value,
                "group": ix,
            }
            


@click.command()
@click.argument("resolver_path", type=InFile)
@click.argument("dataset_path", type=InFile)
@click.argument("outfile", type=OutFile)
@click.option("--log-level", default="INFO")
def main(resolver_path: Path, dataset_path: Path, outfile: Path, log_level: str):
    configure_logging(level=log_level)
    dataset = load_dataset_from_path(dataset_path)

    with open(outfile, "w") as out_fh:
        for pair in generate_groups(resolver_path, dataset):
            out_fh.write(json.dumps(pair) + "\n")


if __name__ == "__main__":
    main()
