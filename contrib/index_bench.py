import importlib
from typing import Type
import click
from nomenklatura.index.common import BaseIndex

from zavod.cli import _load_dataset
from zavod.meta import get_catalog, get_multi_dataset, load_dataset_from_path
from zavod.store import get_store, get_view
from followthemoney.cli.util import InPath


def get_class(fq_class: str) -> Type[BaseIndex]:
    module_name, class_name = fq_class.rsplit(".", 1)
    module = importlib.import_module(module_name)
    class_ = getattr(module, class_name)
    if not issubclass(class_, BaseIndex):
        raise click.BadParameter(f"Class {fq_class} must be a subclass of nomenklatura.index.BaseIndex")
    return class_


@click.option("--exit-after-entities", is_flag=False, default=None, type=int)
@click.argument("fq_class", type=str)
@click.argument("dataset_path", type=InPath)
@click.command()
def main(dataset_path: str, fq_class: str, exit_after_entities: Optional[int] = None):
    class_ = get_class(fq_class)
    dataset = _load_dataset(dataset_path)
    subject_view = get_view(get_multi_dataset(dataset.inputs))
    target_dataset_name = dataset.config.pop("dataset")
    target_dataset = get_catalog().require(target_dataset_name)
    store = get_store(target_dataset)
    target_view = store.default_view(external=False)
    print(f"Creating {fq_class} index")
    index: BaseIndex = class_(target_view)
    print(f"Indexing...")
    index.build()
    print("Matching entities")
    for entity_idx, entity in enumerate(subject_view.entities()):
        if not entity.schema.matchable:
            continue
        print("Entity:", entity.id, entity.get("name"))
        for ident, score in index.match(entity)[:10]:
            candidate = target_view.get_entity(ident.id)
            print("Candidate", "%.3f" % score, candidate.get("name"))


if __name__ == "__main__":
    main()
