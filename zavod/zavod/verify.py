from followthemoney.types import registry

from zavod.context import Context
from zavod.meta.dataset import Dataset
from zavod.store import View
from zavod.entity import Entity


def check_dangling_references(context: Context, view: View, entity: Entity) -> None:
    for prop in entity.iterprops():
        if prop.type != registry.entity:
            return
        for other_id in entity.get(prop):
            if view.has_entity(other_id):
                return
    context.log.error(
        f"{entity.id} references missing id {other_id}"
    )


def check_self_references(context: Context, view: View, entity: Entity) -> None:
    if not entity.schema.is_a("Thing"):
        return
    for prop in entity.iterprops():
        if prop.type != registry.entity:
            continue
        if prop.range.is_a("Thing"):
            for other_id in entity.get(prop):
                if other_id == entity.id:
                    context.log.error(
                        f"{entity.id} references itself on {prop.name}."
                    )
        elif prop.range.is_a("Interval"):
            for other_id in entity.get(prop):
                other = view.get_entity(other_id)
                for other_prop in other.iterprops():
                    if other_prop.type != registry.entity:
                        return
                    if other_prop.reverse == prop:
                        return
                    if entity.id in other.get(other_prop):
                        context.log.error(
                            f"{entity.id} references itself on {prop.name} via {other.id}'s {other_prop.name}."
                        )
                

def verify_dataset(dataset: Dataset, view: View) -> None:
    try:
        context = Context(dataset)
        context.begin(clear=False)
        for idx, entity in enumerate(view.entities()):
            check_dangling_references(context, view, entity)
            check_self_references(context, view, entity)
            # check_topicless_target(context, view, entity)

            if idx > 0 and idx % 10000 == 0:
                context.log.info("Verified %s entities..." % idx, dataset=dataset.name)
    finally:
        context.close()
