from typing import Set

from followthemoney import registry

from zavod import Context, Entity
from zavod.meta import get_multi_dataset
from zavod.store import get_store, View
from zavod.integration import get_dataset_linker


def non_graph_topics(context: Context, entity: Entity) -> Set[str]:
    topic_stmts = entity.get_statements("topics")
    return {s.value for s in topic_stmts if s.dataset != context.dataset.name}


def emit_patch(context: Context, entity: Entity, topic: str) -> None:
    patch = context.make(entity.schema)
    patch.id = entity.id
    patch.add("topics", topic)
    context.emit(patch)


def analyze_entity(context: Context, view: View, entity: Entity) -> None:
    topics = entity.get_type_values(registry.topic)
    for prop, adjacent in view.get_adjacent(entity):
        asch = adjacent.schema
        if prop.reverse is None or not asch.edge:
            continue

        # For when the other entity is on the other side of an edge
        other_prop = (
            asch.source_prop if prop.reverse == asch.target_prop else asch.target_prop
        )
        if other_prop is None:
            continue

        if "role.pep" in topics and adjacent.schema.is_a("Family"):
            for other_id in adjacent.get(other_prop):
                other = view.get_entity(other_id)
                if other is None or not other.schema.is_a("Person"):
                    continue
                other_topics = non_graph_topics(context, other)
                if other_topics.intersection({"role.rca", "role.pep"}):
                    continue
                context.log.info(
                    "Adding topic: role.rca",
                    pep=entity.caption,
                    pep_id=entity.id,
                    rca=other.caption,
                    rca_id=other.id,
                    topics=other_topics,
                )
                emit_patch(context, other, "role.rca")

        # Family is eternal, business is time-bound:
        if len(adjacent.get("endDate", quiet=True)) > 0:
            context.log.info(
                "Skipping entity with end date", adjacent=adjacent.id, entity=entity.id
            )
            continue

        if "sanction" in topics:
            if entity.schema.is_a("Security"):
                continue
            if adjacent.schema.name not in (
                "Ownership",
                "Directorship",
                "Membership",
                "Employment",
                "Associate",
                "Family",
                "Succession",
            ):
                continue
            for other_id in adjacent.get(other_prop):
                other = view.get_entity(other_id)
                if other is None:
                    continue
                other_topics = non_graph_topics(context, other)
                if other_topics.intersection({"sanction", "sanction.linked"}):
                    continue
                context.log.info(
                    "Adding topic: sanction.linked",
                    sanctioned=entity.caption,
                    sanctioned_id=entity.id,
                    linked=other.caption,
                    linked_id=other.id,
                    topics=other_topics,
                )
                emit_patch(context, other, "sanction.linked")

        if (
            "sanction.linked" in topics
            and adjacent.schema.is_a("Ownership")
            and prop.reverse.name == "owner"
        ):
            for other_id in adjacent.get(other_prop):
                other = view.get_entity(other_id)
                if other is None:
                    continue
                other_topics = non_graph_topics(context, other)
                if other_topics.intersection({"sanction", "sanction.linked"}):
                    continue
                context.log.info(
                    "Adding topic: sanction.linked",
                    sanctioned=entity.caption,
                    sanctioned_id=entity.id,
                    linked=other.caption,
                    linked_id=other.id,
                    topics=other_topics,
                )
                emit_patch(context, other, "sanction.linked")


def crawl(context: Context) -> None:
    scope = get_multi_dataset(context.dataset.inputs)
    linker = get_dataset_linker(scope)
    store = get_store(scope, linker)
    store.sync()
    view = store.view(scope)

    for entity_idx, entity in enumerate(view.entities()):
        if entity_idx > 0 and entity_idx % 1000 == 0:
            context.log.info("Processed %s entities" % entity_idx)
        analyze_entity(context, view, entity)
