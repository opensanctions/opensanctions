# from typing import Dict, Optional, Set, Tuple, Generator
# from itertools import combinations
# from collections import defaultdict
import json
from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Identifier, Resolver, Linker, Edge

#
# from zavod.meta import Dataset
# from zavod.entity import Entity
# from zavod.store import get_store, Store
from zavod import settings
from zavod.dedupe import get_resolver
from zavod.entity import Entity
from zavod.logs import get_logger, configure_logging

# from zavod.dedupe import get_resolver
# from opensanctions.core.catalog import get_catalog

import click
import logging
from pathlib import Path
from typing import Generator, Optional, Set, Tuple

from zavod.logs import get_logger, configure_logging
from zavod.meta import load_dataset_from_path
from zavod.meta.dataset import Dataset
from zavod.store import Store, View, get_store

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
    dataset: Dataset,
) -> Generator[Tuple[Entity, Entity, Judgement], None, None]:
    resolver = ChronAggResolver(Path(settings.RESOLVER_PATH))
    store = get_store(dataset, Linker({}))
    store.sync()
    view = store.view(dataset, external=False)

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
            continue

        yield source, target, judgement


@click.command()
@click.argument("dataset_path", type=InFile)
@click.argument("outfile", type=OutFile)
def main(dataset_path: Path, outfile: Path):
    configure_logging(level=logging.INFO)
    dataset = load_dataset_from_path(dataset_path)

    with open(outfile, "w") as out_fh:
        for idx, (source, target, judgement) in enumerate(generate_pairs(dataset)):
            if idx > 0 and idx % 1000 == 0:
                log.info("Exported %d pairs..." % idx)

            out_fh.write(
                json.dumps(
                    {
                        "left": source.to_dict(),
                        "right": target.to_dict(),
                        "judgement": judgement.value,
                    }
                )
                + "\n"
            )


if __name__ == "__main__":
    main()


# def get_parts(
#     resolver: Resolver, datasets: Dict[str, Set[Dataset]], id: str
# ) -> Generator[Tuple[str, Dataset], None, None]:
#     canonical_id = resolver.get_canonical(id)
#     for ref in resolver.get_referents(canonical_id):
#         if ref.startswith(Identifier.PREFIX):
#             continue
#         for ds in datasets.get(ref, []):
#             yield ref, ds
#
#
# def get_partial(
#     resolver: Resolver, store: Store, spec: Tuple[str, Dataset]
# ) -> Optional[Entity]:
#     id, ds = spec
#     loader = store.view(ds, external=True)
#     canonical = resolver.get_canonical(id)
#     entity = loader.get_entity(canonical)
#     if entity is None:
#         return None
#     entity.id = id
#     return entity
#
#
# def export_training_pairs(scope: Dataset):
#     resolver = get_resolver()
#     catalog = get_catalog()
#     datasets: Dict[str, Set[Dataset]] = defaultdict(set)
#     with engine_read() as conn:
#         for entity_id, ds in entities_datasets(conn, scope):
#             if ds not in scope.leaf_names:
#                 continue
#             dsa = catalog.get(ds)
#             if dsa is not None:
#                 datasets[entity_id].add(dsa)
#
#     log.info("Loaded %d entity ID mappings..." % len(datasets))
#     pairs: Dict[Tuple[Tuple[str, Dataset], Tuple[str, Dataset]], Judgement] = {}
#     judgements: Dict[Judgement, int] = defaultdict(int)
#     for canonical_id in resolver.canonicals():
#         parts = list(get_parts(resolver, datasets, canonical_id))
#         for left, right in combinations(parts, 2):
#             left, right = max(left, right), min(left, right)
#             pairs[(left, right)] = Judgement.POSITIVE
#             judgements[Judgement.POSITIVE] += 1
#         for edge in resolver.nodes[canonical_id]:
#             if edge.judgement in (Judgement.NEGATIVE, Judgement.UNSURE):
#                 source_canonical = resolver.get_canonical(edge.source)
#                 other = edge.target if source_canonical == canonical_id else edge.source
#                 for other_part in get_parts(resolver, datasets, other):
#                     for part in parts:
#                         part, other_part = max(part, other_part), min(part, other_part)
#                         # pairs[(part, other_part)] = edge.judgement
#                         # Export unsure as negative:
#                         pairs[(part, other_part)] = edge.judgement
#                         judgements[edge.judgement] += 1
#
#     log.info(
#         "Computed %d potential pairs..." % len(pairs),
#         positive=judgements.get(Judgement.POSITIVE, 0),
#         negative=judgements.get(Judgement.NEGATIVE, 0),
#         unsure=judgements.get(Judgement.UNSURE, 0),
#     )
#     store = get_store(scope, external=True)
#     for idx, ((left, right), judgement) in enumerate(pairs.items()):
#         if idx > 0 and idx % 10000 == 0:
#             log.info("Exported %d pairs..." % idx)
#         left_entity = get_partial(resolver, store, left)
#         right_entity = get_partial(resolver, store, right)
#         if left_entity is None or right_entity is None:
#             continue
#
#         yield {
#             "left": left_entity.to_dict(),
#             "right": right_entity.to_dict(),
#             "judgement": judgement,
#         }
