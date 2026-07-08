"""Generate matcher training pairs by chronological replay of resolver judgements.

One row is emitted per human judgement edge, where both sides are the
partially-merged clusters as the resolver knew them at judgement time. See
DATA.md (copied into the output directory) for the data semantics that
downstream consumers must respect.
"""

import hashlib
import logging
import time
from collections import Counter
from pathlib import Path
from shutil import copyfile
from typing import Any, Dict, List, Optional, Tuple

import click
import orjson

from nomenklatura.db import make_session
from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Linker, Resolver
from nomenklatura.resolver.edge import Edge
from nomenklatura.resolver.identifier import Identifier

from zavod import settings
from zavod.entity import Entity
from zavod.integration.dedupe import get_resolver
from zavod.logs import configure_logging, get_logger
from zavod.meta import get_catalog, get_multi_dataset
from zavod.store import View, get_store

log = get_logger(Path(__file__).stem)

FORMAT_VERSION = 1

# Judgements produced by matching algorithms must not become training data for
# a matcher: that is a feedback loop. Their edges still shape replayed cluster
# states (they merged entities in reality), so they are registered, not emitted.
AUTOMATION_USERS = {"zavod/logic", "zavod/xref"}

EMIT_JUDGEMENTS = (Judgement.POSITIVE, Judgement.NEGATIVE, Judgement.UNSURE)

# These datasets have particularly jiggy data quality, so we don't want to
# derive training data from judgements linked to them. They are excluded from
# the store scope, so their pairs surface in the "skipped_missing" counter.
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

OutDir = click.Path(file_okay=False, writable=True, path_type=Path)


class UnionFind:
    """Track connected components of entity identifiers."""

    def __init__(self) -> None:
        self.parent: Dict[str, str] = {}
        self.members: Dict[str, List[str]] = {}

    def find(self, node: str) -> str:
        root = node
        while self.parent.get(root, root) != root:
            root = self.parent[root]
        while node != root:
            self.parent[node], node = root, self.parent.get(node, root)
        return root

    def union(self, left: str, right: str) -> None:
        lroot = self.find(left)
        rroot = self.find(right)
        if lroot == rroot:
            return
        lmembers = self.members.setdefault(lroot, [lroot])
        rmembers = self.members.setdefault(rroot, [rroot])
        if len(lmembers) < len(rmembers):
            lroot, rroot = rroot, lroot
            lmembers, rmembers = rmembers, lmembers
        self.parent[rroot] = lroot
        lmembers.extend(rmembers)
        del self.members[rroot]

    def component(self, node: str) -> List[str]:
        return self.members.get(self.find(node), [node])


def load_edges(resolver: Resolver[Entity]) -> List[Edge]:
    """Load live judged edges in deterministic chronological order."""
    edges = resolver._live_edges()
    edges = [e for e in edges if e.judgement != Judgement.NO_JUDGEMENT]
    # Null timestamps sort last; the key tiebreak makes replay order fully
    # deterministic rather than dependent on database row order.
    edges.sort(key=lambda e: (e.created_at or "XXXX", e.target.id, e.source.id))
    return edges


def group_label(groups: UnionFind, node: str, cache: Dict[str, str]) -> str:
    """Return the stable component label: its smallest identifier."""
    root = groups.find(node)
    label = cache.get(root)
    if label is None:
        label = min(groups.component(root))
        cache[root] = label
    return label


def resolve_side(
    view: View, replay: UnionFind, node_id: str
) -> Tuple[Optional[Entity], Optional[str]]:
    """Merge the replay-time cluster around node_id from the store view.

    Returns (cluster, None) on success or (None, skip_reason) when the side
    cannot be used for a training pair."""
    saw_address = False
    cluster: Optional[Entity] = None
    members = sorted(replay.component(node_id))
    for member in members:
        entity = view.get_entity(member)
        if entity is None:
            continue
        if entity.schema.is_a("Address"):
            saw_address = True
            continue
        if cluster is None:
            cluster = entity
            continue
        try:
            cluster = cluster.merge(entity)
        except Exception as exc:
            log.warning(
                "Error merging entities: %s" % exc,
                cluster=cluster.id,
                member=entity.id,
            )
            return None, "merge_error"
    if cluster is None:
        return None, "address" if saw_address else "missing"
    cluster.id = max(Identifier.get(m) for m in members).id
    return cluster, None


def hash_user(user: Optional[str]) -> Optional[str]:
    if user is None:
        return None
    return hashlib.sha256(user.encode("utf-8")).hexdigest()[:12]


def entity_dict(entity: Entity) -> Dict[str, Any]:
    """Serialize an entity with sorted value lists for byte-stable output."""
    properties = {prop: sorted(values) for prop, values in entity.properties.items()}
    return {"id": entity.id, "schema": entity.schema.name, "properties": properties}


def make_row(edge: Edge, group: str, left: Entity, right: Entity) -> Dict[str, Any]:
    return {
        "format_version": FORMAT_VERSION,
        "left": entity_dict(left),
        "right": entity_dict(right),
        "judgement": edge.judgement.value,
        "group": group,
        "source_id": edge.source.id,
        "target_id": edge.target.id,
        "created_at": edge.created_at,
        "score": edge.score,
        "user": hash_user(edge.user),
        "left_datasets": sorted(left.datasets),
        "right_datasets": sorted(right.datasets),
    }


def component_stats(
    groups: UnionFind, cache: Dict[str, str], pairs_per_group: Counter
) -> Dict[str, Any]:
    """Report the component size distribution; the split policy for very large
    components is decided downstream, this only measures."""
    node_counts = sorted((len(m) for m in groups.members.values()), reverse=True)
    edge_counts = sorted(pairs_per_group.values(), reverse=True)

    def percentile(values: List[int], share: float) -> int:
        if not values:
            return 0
        return values[min(len(values) - 1, int(len(values) * share))]

    largest = [
        {"group": group, "pairs": count}
        for group, count in pairs_per_group.most_common(10)
    ]
    return {
        "components": len(groups.members),
        "components_with_emitted_pairs": len(pairs_per_group),
        "nodes": sum(node_counts),
        "node_count_percentiles": {
            "p50": percentile(node_counts, 0.5),
            "p90": percentile(node_counts, 0.1),
            "p99": percentile(node_counts, 0.01),
            "max": node_counts[0] if node_counts else 0,
        },
        "emitted_pairs_percentiles": {
            "p50": percentile(edge_counts, 0.5),
            "p90": percentile(edge_counts, 0.1),
            "p99": percentile(edge_counts, 0.01),
            "max": edge_counts[0] if edge_counts else 0,
        },
        "largest_by_emitted_pairs": largest,
    }


def generate(scope: str, outdir: Path) -> Dict[str, Any]:
    started = time.monotonic()
    dataset_scope = get_catalog().require(scope)
    datasets = [d for d in dataset_scope.datasets if d.name not in IGNORE_DATASETS]
    datasets = [d for d in datasets if not d.is_collection]
    dataset = get_multi_dataset([d.name for d in datasets])

    store = get_store(dataset, Linker({}))
    store.sync()
    view = store.view(dataset, external=True)

    with make_session() as session:
        resolver = get_resolver(session)
        edges = load_edges(resolver)
    log.info(f"Replaying {len(edges)} edges...")

    # Components span every judged edge - negatives and automation included -
    # because their pairs share evidence with the component's other pairs.
    # This is what a leakage-safe train/test split downstream must respect.
    groups = UnionFind()
    for edge in edges:
        groups.union(edge.source.id, edge.target.id)

    replay = UnionFind()
    group_cache: Dict[str, str] = {}
    skips: Counter[str] = Counter()
    emitted: Counter[str] = Counter()
    pairs_per_group: Counter[str] = Counter()

    outdir.mkdir(parents=True, exist_ok=True)
    with open(outdir / "pairs.jsonl", "wb") as fh:
        for idx, edge in enumerate(edges):
            if idx > 0 and idx % 10000 == 0:
                log.info(
                    f"Replayed {idx} edges...",
                    emitted=sum(emitted.values()),
                    skipped=sum(skips.values()),
                )
            if edge.judgement not in EMIT_JUDGEMENTS:
                skips["judgement"] += 1
            elif edge.user in AUTOMATION_USERS:
                skips["automation"] += 1
            else:
                left, left_skip = resolve_side(view, replay, edge.target.id)
                right, right_skip = resolve_side(view, replay, edge.source.id)
                reason = left_skip or right_skip
                if reason is not None or left is None or right is None:
                    skips[reason or "missing"] += 1
                else:
                    group = group_label(groups, edge.source.id, group_cache)
                    row = make_row(edge, group, left, right)
                    fh.write(orjson.dumps(row, option=orjson.OPT_SORT_KEYS))
                    fh.write(b"\n")
                    emitted[edge.judgement.value] += 1
                    pairs_per_group[group] += 1
            # Register after emission: the pair reflects the cluster state the
            # decider saw, before their own judgement was applied.
            if edge.judgement == Judgement.POSITIVE:
                replay.union(edge.source.id, edge.target.id)

    summary = {
        "format_version": FORMAT_VERSION,
        "scope": scope,
        "generated_at": settings.RUN_TIME_ISO,
        "edges_replayed": len(edges),
        "emitted": dict(sorted(emitted.items())),
        "emitted_total": sum(emitted.values()),
        "skipped": dict(sorted(skips.items())),
        "components": component_stats(groups, group_cache, pairs_per_group),
        "elapsed_seconds": round(time.monotonic() - started, 1),
    }
    with open(outdir / "summary.json", "wb") as fh:
        fh.write(
            orjson.dumps(summary, option=orjson.OPT_SORT_KEYS | orjson.OPT_INDENT_2)
        )
        fh.write(b"\n")
    copyfile(Path(__file__).parent / "DATA.md", outdir / "DATA.md")
    return summary


@click.command()
@click.argument("scope", type=str, default="default")
@click.argument("outdir", type=OutDir, default=settings.DATA_PATH / "matcher_training")
@click.option("--log-level", default="INFO")
def main(scope: str, outdir: Path, log_level: str) -> None:
    configure_logging(level=logging.getLevelNamesMapping()[log_level])
    summary = generate(scope, outdir)
    log.info(
        "Done.",
        emitted=summary["emitted_total"],
        skipped=summary["skipped"],
        elapsed=summary["elapsed_seconds"],
    )


if __name__ == "__main__":
    main()
