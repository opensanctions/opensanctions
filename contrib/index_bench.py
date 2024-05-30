import importlib
from typing import Optional, Type
import click
from nomenklatura.index.common import BaseIndex
from datetime import datetime
import logging
from followthemoney.cli.util import InPath
import sys

from zavod.cli import _load_dataset
from zavod.meta import get_catalog, get_multi_dataset, load_dataset_from_path
from zavod.store import get_store, get_view


log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
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
@click.argument("fq_class", type=str)
@click.argument("dataset_path", type=InPath)
@click.command()
def main(
    dataset_path: str,
    fq_class: str,
    exit_after_entities: Optional[int] = None,
    log_entities: bool = False,
):
    subjects_searched = 0
    class_ = get_class(fq_class)
    dataset = _load_dataset(dataset_path)
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
        subjects_searched += 1
    matching_end_ts = datetime.now()
    log.info(f"Blocking took {matching_end_ts - matching_start_ts}")


if __name__ == "__main__":
    main()
