from followthemoney.types import registry

from zavod.context import Context
from zavod.meta.dataset import Dataset
from zavod.store import View


def find_dangling(context: Context, dataset: Dataset, view: View) -> None:
    for idx, entity in enumerate(view.entities()):
        for prop in entity.iter_properties():
            if prop.type == registry.entity:
                for other_id in entity.get(prop):
                    if not view.has_entity(other_id):
                        context.log.error(
                            "{entity.id} references missing id" % other_id
                        )

        if idx > 0 and idx % 10000 == 0:
            context.log.info("Verified %s entities..." % idx, dataset=dataset.name)


def verify_dataset(dataset: Dataset, view: View) -> None:
    try:
        context = Context(dataset)
        context.begin(clear=False)
        find_dangling(context, dataset, view)
    finally:
        context.close()
