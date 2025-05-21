from typing import Optional, Set

from zavod import Context, Entity
from zavod.integration import get_dataset_linker
from zavod.meta import get_multi_dataset
from zavod.stateful.positions import categorise_many
from zavod.store import get_store


def make_keyword(topics: Set[str]) -> Optional[str]:
    if "gov.national" in topics:
        return "National government"
    if "gov.state" in topics:
        return "State government"
    if "gov.igo" in topics:
        return "International organization"
    if "gov.muni" in topics:
        return "Local government"
    return None


def emit_keywords(context: Context, person: Entity, topics: Set[str]) -> None:
    proxy = context.make("Person")
    proxy.id = person.id
    proxy.add("keywords", make_keyword(topics))
    if proxy.get("keywords"):
        context.emit(proxy)


def analyze_position(context: Context, entity: Entity) -> Set[str]:
    """Analyze a position entity and emit the categorisation."""
    topics = set()

    assert entity.id is not None
    entity_ids = set(entity.referents)
    entity_ids.add(entity.id)

    for categorisation in categorise_many(context, entity_ids):
        if not categorisation.topics:
            continue

        proxy = context.make("Position")
        proxy.id = entity.id
        # emit the topics for each referent under that ID
        proxy.add("topics", categorisation.topics)
        if proxy.get("topics"):
            context.emit(proxy)

        # collect all the topics for the position
        topics.update(categorisation.topics)
    return topics


def crawl(context: Context) -> None:
    scope = get_multi_dataset(context.dataset.inputs)
    linker = get_dataset_linker(scope)
    store = get_store(scope, linker)
    store.sync()
    view = store.view(scope)
    position_count = 0

    for entity_idx, entity in enumerate(view.entities()):
        if entity_idx > 0 and entity_idx % 1000 == 0:
            context.log.info("Processed %s entities" % entity_idx)

        if entity.schema.is_a("Occupancy"):
            topics = set()

            # Only one expected, but handle more.
            for position_id in entity.get("post"):
                position = view.get_entity(position_id)
                if position is None:
                    continue
                topics.update(analyze_position(context, position))
                position_count += 1
                if position_count % 1000 == 0:
                    context.log.info("Analyzed %s positions" % position_count)
                    context.flush()

            if not topics:
                continue

            # Only one expected, but handle more.
            for person_id in entity.get("holder"):
                person = view.get_entity(person_id)
                if person is None:
                    continue
                emit_keywords(context, person, topics)
