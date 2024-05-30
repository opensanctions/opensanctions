import importlib
from typing import Dict, Optional, Set, Type
import click
from nomenklatura.index.common import BaseIndex
from datetime import datetime
import logging
from followthemoney.cli.util import InPath
import sys

from zavod.archive import STATEMENTS_FILE, _read_fh_statements, backfill_resource, dataset_resource_path, dataset_state_path, get_dataset_resource, iter_dataset_statements
from zavod.cli import _load_dataset
from zavod.meta import get_catalog, get_multi_dataset, load_dataset_from_path
from zavod.store import get_store, get_view


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)


def get_class(fq_class: str) -> Type[BaseIndex]:
    module_name, class_name = fq_class.rsplit(".", 1)
    module = importlib.import_module(module_name)
    class_ = getattr(module, class_name)
    if not issubclass(class_, BaseIndex):
        raise click.BadParameter(
            f"Class {fq_class} must be a subclass of nomenklatura.index.BaseIndex"
        )
    return class_


@click.option("--exit-after-entities", is_flag=False, default=None, type=int)
@click.option("--log-entities", is_flag=True, default=False)
@click.option("--compare-with-prod", is_flag=True, default=False)
@click.argument("fq_class", type=str)
@click.argument("dataset_path", type=InPath)
@click.command()
def main(
    dataset_path: str,
    fq_class: str,
    exit_after_entities: Optional[int] = None,
    log_entities: bool = False,
    compare_with_prod: bool = False,
):
    dataset = _load_dataset(dataset_path)

    if compare_with_prod:
        baseline: Dict[str, bool] = dict()
        candidates_in_baseline = set()
        positive_candidates = 0
        external_candidates = 0
        statements_path = dataset_resource_path(dataset.name, STATEMENTS_FILE)
        backfill_resource(dataset, STATEMENTS_FILE, statements_path)
        log.info("Loading baseline data")
        for stmt in iter_dataset_statements(dataset, external=True):
            # Don't do any counting here because statements aren't deduped.
            if stmt.prop == "id":
                baseline[stmt.canonical_id] = stmt.external
        log.info(f"Loaded {len(baseline)} baseline entity IDs")

    subjects_searched = 0
    class_ = get_class(fq_class)
    subject_view = get_view(get_multi_dataset(dataset.inputs))
    target_dataset_name = dataset.config.pop("dataset")
    target_dataset = get_catalog().require(target_dataset_name)
    store = get_store(target_dataset)
    target_view = store.default_view(external=False)

    log.info(f"Creating {fq_class} index")
    index_start_ts = datetime.now()
    index: BaseIndex = class_(target_view)
    log.info(f"Indexing {target_dataset}")
    index.build()
    index_end_ts = datetime.now()
    log.info(f"Indexing took {index_end_ts - index_start_ts}")

    log.info("Blocking entities")
    matching_start_ts = datetime.now()
    for entity in subject_view.entities():
        if exit_after_entities is not None and subjects_searched >= exit_after_entities:
            break
        if not entity.schema.matchable:
            continue
        if log_entities:
            log.info(f"Entity {subjects_searched}: {entity.id} {entity.get("name")}")
        
        for ident, score in index.match(entity)[:10]:
            if log_entities:
                candidate = target_view.get_entity(ident.id)
                log.info("Candidate %.3f %s %r", score, candidate.id, candidate.get("name"))
            
            if compare_with_prod:
                if ident.id in baseline:
                    candidates_in_baseline.add(ident.id)
                    if baseline[ident.id]:
                        external_candidates += 1
                    else:
                        positive_candidates += 1
                
        subjects_searched += 1
    matching_end_ts = datetime.now()
    log.info(f"Blocking took {matching_end_ts - matching_start_ts}")

    if compare_with_prod:
        non_candidate_positives: Set[str] = set()
        non_candidate_externals: Set[str] = set()
        total_positives = 0
        total_externals = 0

        for entity_id, external in baseline.items():
            if external:
                total_externals += 1
            else:
                total_positives += 1
            if entity_id not in candidates_in_baseline:
                if external:
                    non_candidate_externals.add(entity_id)
                else:
                    non_candidate_positives.add(entity_id)

        log.info(f"Positive matches as candidates: %d/%d (%.2f%%)", positive_candidates, total_positives, positive_candidates / total_positives * 100)
        log.info(f"Externals as candidates: %d/%d (%.2f%%)", external_candidates, total_externals, external_candidates / total_externals * 100)

        with open("positive_match_misses.txt", "w") as f:
            for entity_id in non_candidate_positives:
                f.write(entity_id + "\n")
        with open("external_misses.txt", "w") as f:
            for entity_id in non_candidate_externals:
                f.write(entity_id + "\n")


if __name__ == "__main__":
    main()
