from collections import defaultdict
from typing import Set, Optional

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
    OccupancyStatus.CURRENT.value: "current",
    OccupancyStatus.ENDED.value: "past",
    OccupancyStatus.UNKNOWN.value: "unknown status",
}


def get_best_status(statuses: Set[str]) -> OccupancyStatus:
    """Get the best status from a list of statuses.

    If we've seen a CURRENT, that trumps all. If not, we prefer UNKNOWN over ENDED to play it safe.
    We emit ENDED only if we're totally sure."""
    if OccupancyStatus.CURRENT.value in statuses:
        return OccupancyStatus.CURRENT

    if OccupancyStatus.UNKNOWN.value in statuses:
        return OccupancyStatus.UNKNOWN

    if OccupancyStatus.ENDED.value in statuses:
        return OccupancyStatus.ENDED

    # Usually this means len(statuses) == 0, but could also be an unexpected status value
    return OccupancyStatus.UNKNOWN


def format_influence_label(topic: str, status: OccupancyStatus) -> Optional[str]:
    level_label = INFLUENCE_TOPIC_LABELS.get(topic, None)
    status_label = OCCUPANCY_STATUS_LABELS.get(status, None)
    if status_label is None or level_label is None:
        return None

    return f"{level_label} ({status_label})"


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

            topic_to_seen_statuses = defaultdict(set)

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
                        topic_to_seen_statuses[topic].add(occupancy.get("status"))

            # For each topic, we determine the best status seen and build the label from it.
            influence_labels = [
                format_influence_label(topic, get_best_status(seen_statuses))
                for topic, seen_statuses in topic_to_seen_statuses.items()
            ]
            person_proxy = context.make("Person")
            person_proxy.id = entity.id
            person_proxy.add("classification", influence_labels)
            context.emit(person_proxy)
