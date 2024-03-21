from followthemoney.types import registry

from zavod.context import Context
from zavod.meta.dataset import Dataset
from zavod.store import View
from zavod.entity import Entity


def check_dangling_references(context: Context, view: View, entity: Entity) -> None:
    for prop in entity.iterprops():
        if prop.type != registry.entity:
            continue
        for other_id in entity.get(prop):
            if view.has_entity(other_id):
                continue
            context.log.error(
                f"{entity.id} property {prop.name} references missing id {other_id}."
            )


# FollowTheMoney prevents direct self-references so we check 1 level deep
def check_self_references(context: Context, view: View, entity: Entity) -> None:
    if not entity.schema.is_a("Thing"):
        return
    for prop, other in view.get_adjacent(entity):
        #if prop.range is not None and not prop.range.is_a("Interval"):
        #    continue
        for other_prop in other.iterprops():
            if other_prop.type != registry.entity:
                continue
            if other_prop.reverse == prop:
                continue
            if entity.id in other.get(other_prop):
                context.log.error(
                    f"{entity.id} references itself via {prop.name} -> {other.id} -> {other_prop.name}."
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
