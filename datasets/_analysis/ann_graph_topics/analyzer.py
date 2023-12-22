from followthemoney.types import registry

from zavod import Context
from zavod.meta import get_multi_dataset
from zavod.store import get_view


def crawl(context: Context) -> None:
    view = get_view(get_multi_dataset(context.dataset.inputs))

    for entity_idx, entity in enumerate(view.entities()):
        if entity_idx > 0 and entity_idx % 1000 == 0:
            context.log.info("Processed %s entities" % entity_idx)

        topics = entity.get_type_values(registry.topic)
        for prop, adjacent in view.get_adjacent(entity):
            asch = adjacent.schema
            if prop.reverse is None or not asch.edge:
                continue

            other_prop = (
                asch.source_prop
                if prop.reverse == asch.target_prop
                else asch.target_prop
            )
            if other_prop is None:
                continue

            if "role.pep" in topics and adjacent.schema.is_a("Family"):
                for other_id in adjacent.get(other_prop):
                    other = view.get_entity(other_id)
                    if other is None or not other.schema.is_a("Person"):
                        continue
                    other_topic_stmts = other.get_statements("topics")
                    other_topics = [
                        s.value
                        for s in other_topic_stmts
                        if s.dataset != context.dataset.name
                    ]
                    if "role.rca" in other_topics or "role.pep" in other_topics:
                        continue
                    context.log.info(
                        "Adding topic: role.rca",
                        pep=entity.caption,
                        pep_id=entity.id,
                        rca=other.caption,
                        rca_id=other.id,
                        topics=other_topics,
                    )
                    patch = context.make(other.schema)
                    patch.id = other.id
                    patch.add("topics", "role.rca")
                    context.emit(patch)

            if "sanction" in topics:
                for other_id in adjacent.get(other_prop):
                    other = view.get_entity(other_id)
                    if other is None:
                        continue
                    other_topic_stmts = other.get_statements("topics")
                    other_topics = [
                        s.value
                        for s in other_topic_stmts
                        if s.dataset != context.dataset.name
                    ]
                    if "sanction" in other_topics or "sanction.linked" in other_topics:
                        continue
                    context.log.info(
                        "Adding topic: sanction.linked",
                        sanctioned=entity.caption,
                        sanctioned_id=entity.id,
                        linked=other.caption,
                        linked_id=other.id,
                        topics=other_topics,
                    )
                    patch = context.make(other.schema)
                    patch.id = other.id
                    patch.add("topics", "sanction.linked")
                    context.emit(patch)

            if (
                "sanction.linked" in topics
                and adjacent.schema.is_a("Ownership")
                and prop.reverse.name == "owner"
            ):
                for other_id in adjacent.get(other_prop):
                    other = view.get_entity(other_id)
                    if other is None:
                        continue
                    other_topic_stmts = other.get_statements("topics")
                    other_topics = [
                        s.value
                        for s in other_topic_stmts
                        if s.dataset != context.dataset.name
                    ]
                    if "sanction" in other_topics or "sanction.linked" in other_topics:
                        continue
                    context.log.info(
                        "Adding topic: sanction.linked",
                        sanctioned=entity.caption,
                        sanctioned_id=entity.id,
                        linked=other.caption,
                        linked_id=other.id,
                        topics=other_topics,
                    )
                    patch = context.make(other.schema)
                    patch.id = other.id
                    patch.add("topics", "sanction.linked")
                    context.emit(patch)
