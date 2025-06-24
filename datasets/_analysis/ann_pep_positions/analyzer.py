from collections import defaultdict
from typing import Dict, Set, Optional

from zavod import Context, Entity
from zavod.integration import get_dataset_linker
from zavod.meta import get_multi_dataset
from zavod.stateful.positions import OccupancyStatus, categorise_many
from zavod.store import get_store

INFLUENCE_TOPIC_LABELS = {
    "gov.national": "National government",
    "gov.state": "State government",
    "gov.igo": "International organization",
    "gov.muni": "Local government",
}
OCCUPANCY_STATUS_LABELS = {
    OccupancyStatus.CURRENT: "current",
    OccupancyStatus.ENDED: "past",
    OccupancyStatus.UNKNOWN: "unknown status",
}


def get_best_occupancy_status(occupancy: Entity) -> OccupancyStatus:
    """Get the best occupancy status for a given occupancy entity."""
    statuses = occupancy.get("status")
    # If a given occupancy has status ended and potentially another value, prefer ended.
    # At the time of writing, this can happen when an ENDED and an UNKNOWN were merged.
    if OccupancyStatus.ENDED.value in statuses:
        return OccupancyStatus.ENDED

    if OccupancyStatus.CURRENT.value in statuses:
        return OccupancyStatus.CURRENT

    return OccupancyStatus.UNKNOWN


def get_best_status(statuses: Set[OccupancyStatus]) -> OccupancyStatus:
    """Get the best status from all occupancies for a given influence level"""
    if OccupancyStatus.CURRENT in statuses:
        return OccupancyStatus.CURRENT

    if OccupancyStatus.UNKNOWN in statuses:
        return OccupancyStatus.UNKNOWN

    if OccupancyStatus.ENDED in statuses:
        return OccupancyStatus.ENDED

    # Usually this means len(statuses) == 0, but could also be an unexpected status value
    return OccupancyStatus.UNKNOWN


def format_influence_label(topic: str, status: OccupancyStatus) -> Optional[str]:
    # If it's not an influence topic, we don't want it
    level_label = INFLUENCE_TOPIC_LABELS.get(topic, None)
    status_label = OCCUPANCY_STATUS_LABELS.get(status, None)
    if status_label is None or level_label is None:
        return None

    return f"{level_label} ({status_label})"


def consolidate_influence(
    topic_to_seen_statuses: dict[str, Set[OccupancyStatus]],
) -> list[str]:
    formatted = [
        format_influence_label(topic, get_best_status(seen_statuses))
        for topic, seen_statuses in topic_to_seen_statuses.items()
    ]
    return [f for f in formatted if f is not None]


def analyze_position(context: Context, entity: Entity) -> Set[str]:
    """Analyze a position entity and emit the categorisation."""
    topics = set()

    assert entity.id is not None
    entity_ids = set(entity.referents)
    entity_ids.add(entity.id)

    for categorisation in categorise_many(context, list(entity_ids)):
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
    pep_count = 0

    for entity_idx, entity in enumerate(view.entities()):
        if entity_idx > 0 and entity_idx % 10000 == 0:
            context.log.info("Processed %s entities" % entity_idx)

        if entity.schema.is_a("Person") and "role.pep" in entity.get("topics"):
            pep_count += 1
            if pep_count > 0 and pep_count % 10000 == 0:
                context.log.info("Processed %s PEPs" % pep_count)

            topic_to_seen_statuses: Dict[str, Set[OccupancyStatus]] = defaultdict(set)

            for prop, adjacent in view.get_adjacent(entity):
                if prop.name != "positionOccupancies":
                    continue

                occupancy = adjacent

                # Only one position expected per occupancy but handle surprises
                for position_id in occupancy.get("post"):
                    position = view.get_entity(position_id)
                    if position is None:
                        continue
                    topics = analyze_position(context, position)
                    if not topics:
                        continue

                    for topic in topics:
                        if topic not in INFLUENCE_TOPIC_LABELS:
                            continue
                        topic_to_seen_statuses[topic].add(
                            get_best_occupancy_status(occupancy)
                        )

            # For each topic, we determine the best status seen and build the label from it.
            influence_labels = consolidate_influence(topic_to_seen_statuses)
            if not influence_labels:
                continue
            person_proxy = context.make("Person")
            person_proxy.id = entity.id
            person_proxy.add("classification", influence_labels)
            context.emit(person_proxy)
