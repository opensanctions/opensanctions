import json
import click
import networkx as nx
from pathlib import Path
from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Resolver, Linker, Edge
from nomenklatura.resolver.identifier import Identifier
from typing import Dict, Optional, Tuple, Generator

from zavod.entity import Entity
from zavod.logs import get_logger, configure_logging
from zavod.meta import load_dataset_from_path
from zavod.meta.dataset import Dataset
from zavod.integration.dedupe import _get_resolver_path
from zavod.store import View, get_store

log = get_logger(__name__)

InFile = click.Path(dir_okay=False, readable=True, file_okay=True, path_type=Path)
OutFile = click.Path(dir_okay=False, writable=True, file_okay=True, path_type=Path)


class ChronoResolver(Resolver):
    @staticmethod
    def restore_chrono(path: Path) -> Generator[Edge, None, None]:
        """Load the edges chronologically but don't add them."""
        edges = []
        with open(path, "r") as fh:
            while True:
                line = fh.readline()
                if not line:
                    break
                edge = Edge.from_line(line)
                edges.append(edge)
        edges.sort(key=lambda e: e.timestamp)
        for edge in edges:
            yield edge

    def register(self, edge: Edge) -> None:
        """Add edges to the resolver."""
        log.debug("Registering edge", edge=edge)
        self._register(edge)
        self.connected.cache_clear()


def lazy_resolve(view: View, resolver: Resolver, id: Identifier) -> Optional[Entity]:
    """
    Get an entity merging all connected entities from the view.
    """
    cluster: Optional[Entity] = None

    for ident in resolver.connected(id):
        entity = view.get_entity(ident.id)
        if entity is None or entity.schema.is_a("Address"):
            continue
        if cluster is None:
            cluster = entity
        else:
            cluster.merge(entity)
    if cluster:
        cluster.id = resolver.get_canonical(id)
    return cluster


def generate_pairs(
    resolver_path: Path,
    dataset: Dataset,
) -> Generator[Tuple[Entity, Entity, Judgement], None, None]:
    chrono_resolver = ChronoResolver()
    store = get_store(dataset, Linker({}))
    store.sync()
    view = store.view(dataset, external=False)

    something_missing_count = 0
    positive_count = 0
    negative_count = 0

    for edge in ChronoResolver.restore_chrono(resolver_path):
        if edge.judgement == Judgement.NO_JUDGEMENT:
            continue

        judgement = edge.judgement
        if judgement == Judgement.UNSURE:
            judgement = Judgement.NEGATIVE

        source = lazy_resolve(view, chrono_resolver, edge.source)
        target = lazy_resolve(view, chrono_resolver, edge.target)

        # Register after loading to get merged entity next time it's referenced.
        chrono_resolver.register(edge)

        missing = []
        if source is None:
            missing.append(edge.source.id)
        if target is None:
            missing.append(edge.target.id)
        if missing:
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


def generate_groups(
    resolver_path: Path, dataset: Dataset
) -> Generator[Dict[str, Entity | str | int], None, None]:
    """
    Each list of pairs is a disconnected subgraph.
    Negative judgements are considered edges too.
    """

    log.info("Loading resolver...")
    complete_resolver = Resolver.load(resolver_path)

    log.info("Generating pairs...")
    g = nx.Graph()
    for i, (source, target, judgement) in enumerate(
        generate_pairs(resolver_path, dataset)
    ):
        if i % 10000 == 0 and i > 0:
            log.info(f"Generated {i} pairs...")

        source_canonical = complete_resolver.get_canonical(source.id)
        target_canonical = complete_resolver.get_canonical(target.id)
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
        for _, _, data in g.subgraph(subgraph_nodes).copy().edges(data=True):
            yield {
                "left": data["source"].to_dict(),
                "right": data["target"].to_dict(),
                "judgement": data["judgement"].value,
                "group": ix,
            }


@click.command()
@click.argument("dataset_path", type=InFile)
@click.argument("outfile", type=OutFile)
@click.option("--log-level", default="INFO")
def main(dataset_path: Path, outfile: Path, log_level: str):
    configure_logging(level=log_level)
    dataset = load_dataset_from_path(dataset_path)
    resolver_path = _get_resolver_path()

    with open(outfile, "w") as out_fh:
        for pair in generate_groups(resolver_path, dataset):
            out_fh.write(json.dumps(pair) + "\n")


if __name__ == "__main__":
    main()
