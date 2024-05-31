import importlib
from typing import Dict, Optional, Set, Type
import click
from nomenklatura.index.common import BaseIndex
from datetime import datetime
import logging
from followthemoney.cli.util import InPath
import sys

from zavod.archive import dataset_state_path
from zavod.cli import _load_dataset
from zavod.meta import get_catalog, get_multi_dataset, load_dataset_from_path
from zavod.store import get_store, get_view

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
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
    baseline_dataset = _load_dataset(dataset_path)
    class_ = get_class(fq_class)

    target_dataset_name = baseline_dataset.config.pop("dataset")
    target_dataset = get_catalog().require(target_dataset_name)
    target_store = get_store(target_dataset)
    target_view = target_store.default_view(external=False)

    if compare_with_prod:
        baseline_internals = set()
        baseline_externals = set()
        positive_candidates = set()
        external_candidates = set()
        
        baseline_store = get_store(baseline_dataset, external=True)
        baseline_view = baseline_store.default_view(external=True)
        for entity in baseline_view.entities():
            if not entity.schema.matchable:
                continue
            for stmt in entity.statements:
                if not statement.pr
                if stmt.external:
                    baseline_externals.add(entity.id)
                else:
                    baseline_internals.add(entity.id)
        log.info(f"Loaded {len(baseline_internals)} baseline internal entity IDs")
        log.info(f"Loaded {len(baseline_externals)} baseline external entity IDs")

    subjects_searched = 0

    subject_view = get_view(get_multi_dataset(baseline_dataset.inputs))
    log.info(f"Enriching {baseline_dataset.inputs} ({subject_view.scope.name})")

    log.info(f"Creating {fq_class} index")
    index_start_ts = datetime.now()
    state_path = dataset_state_path(baseline_dataset.name)
    index: BaseIndex = class_(target_view, state_path, baseline_dataset.config.get("index_options", dict()))

    log.info(f"Indexing {target_dataset.name}")
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

        for ident, score in index.match(entity):
            if log_entities:
                candidate = target_view.get_entity(ident.id)
                log.info(
                    "Candidate %.3f %s %r", score, candidate.id, candidate.get("name")
                )

            if compare_with_prod:
                if ident.id in baseline_internals:
                    positive_candidates.add(ident.id)
                if ident.id in baseline_externals:
                    external_candidates.add(ident.id)

        subjects_searched += 1
    matching_end_ts = datetime.now()
    log.info(f"Blocking took {matching_end_ts - matching_start_ts}")

    if compare_with_prod:

        log.info(
            f"Positive matches as candidates: %d/%d (%.2f%%)",
            len(positive_candidates),
            len(baseline_internals),
            len(positive_candidates) / len(baseline_internals) * 100,
        )
        log.info(
            f"Externals as candidates: %d/%d (%.2f%%)",
            len(external_candidates),
            len(baseline_externals),
            len(external_candidates) / len(baseline_externals) * 100,
        )

        with open("positive_match_misses.txt", "w") as f:
            for entity_id in baseline_internals - positive_candidates:
                f.write(entity_id + "\n")
        with open("external_misses.txt", "w") as f:
            for entity_id in baseline_externals - external_candidates:
                f.write(entity_id + "\n")


if __name__ == "__main__":
    main()
