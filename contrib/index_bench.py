import importlib
from typing import Optional, Type
import click
from nomenklatura.index.common import BaseIndex
from datetime import datetime

from zavod.cli import _load_dataset
from zavod.meta import get_catalog, get_multi_dataset, load_dataset_from_path
from zavod.store import get_store, get_view
from followthemoney.cli.util import InPath


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
@click.option("--show-candidates", is_flag=True, default=False)
@click.option("--candidate-log", type=str, default=None)
@click.argument("fq_class", type=str)
@click.argument("dataset_path", type=InPath)
@click.command()
def main(
    dataset_path: str,
    fq_class: str,
    exit_after_entities: Optional[int] = None,
    show_candidates: bool = False,
    candidate_log: Optional[str] = None,
):
    subjects_searched = 0
    class_ = get_class(fq_class)
    dataset = _load_dataset(dataset_path)
    subject_view = get_view(get_multi_dataset(dataset.inputs))
    target_dataset_name = dataset.config.pop("dataset")
    target_dataset = get_catalog().require(target_dataset_name)
    store = get_store(target_dataset)
    target_view = store.default_view(external=False)
    if candidate_log:
        candidate_log_fw = open(candidate_log, "w")

    print(f"Creating {fq_class} index")
    index_start_ts = datetime.now()
    index: BaseIndex = class_(target_view)
    print(f"Indexing...")
    index.build()
    index_end_ts = datetime.now()
    print(f"Indexing took {index_end_ts - index_start_ts}")

    print("Blocking entities")
    matching_start_ts = datetime.now()
    for entity in subject_view.entities():
        if exit_after_entities is not None and subjects_searched >= exit_after_entities:
            break
        if not entity.schema.matchable:
            continue
        print(f"Entity {subjects_searched}:", entity.id, entity.get("name"))
        if candidate_log:
            candidate_log_fw.write(
                f"Entity {subjects_searched}: {entity.id} {entity.get('name')}\n"
            )
        for ident, score in index.match(entity)[:10]:
            if candidate_log or show_candidates:
                candidate = target_view.get_entity(ident.id)
            if candidate_log:
                candidate_log_fw.write(
                    f"Candidate %.2f: {ident.id} {candidate.get('name')}\n" % score
                )
            if show_candidates:
                print("Candidate", "%.3f" % score, candidate.get("name"))
        subjects_searched += 1
    matching_end_ts = datetime.now()
    print(f"Blocking took {matching_end_ts - matching_start_ts}")


if __name__ == "__main__":
    main()
