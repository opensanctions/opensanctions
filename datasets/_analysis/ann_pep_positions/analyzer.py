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
    """Get the best occupancy status for a given occupancy entity.

    Multiple statuses usually happens for merged occupancies."""
    statuses = occupancy.get("status")
    # If the occupancy has an any ENDED or CURRENT status (i.e. at least one source where we have
    # better information than UNKNOWN), we prefer that.
    # At the time of writing, we don't expect ENDED and CURRENT to be present at the same time.
    if OccupancyStatus.ENDED.value in statuses:
        return OccupancyStatus.ENDED
    if OccupancyStatus.CURRENT.value in statuses:
        return OccupancyStatus.CURRENT

    return OccupancyStatus.UNKNOWN


def get_best_influence_status(statuses: Set[OccupancyStatus]) -> OccupancyStatus:
    """Get the best status from all occupancies for a given influence level."""

    # If any of the occupancies at this influence level is CURRENT, we prefer that
    if OccupancyStatus.CURRENT in statuses:
        return OccupancyStatus.CURRENT

    # If none are CURRENT, but some are UNKNOWN (and maybe some are ENDED as well), we prefer UNKNOWN
    # since we can't say for sure that the influence is ended.
    if OccupancyStatus.UNKNOWN in statuses:
        return OccupancyStatus.UNKNOWN

    # If none are CURRENT or UNKNOWN, that usually means all are ENDED
    if OccupancyStatus.ENDED in statuses:
        return OccupancyStatus.ENDED

    # Usually this means len(statuses) == 0, but could also be an unexpected status value
    return OccupancyStatus.UNKNOWN


def format_influence_label(topic: str, status: OccupancyStatus) -> Optional[str]:
    level_label = INFLUENCE_TOPIC_LABELS.get(topic, None)
    status_label = OCCUPANCY_STATUS_LABELS.get(status, None)
    # If it's not an influence topic, we don't want it
    if status_label is None or level_label is None:
        return None

    return f"{level_label} ({status_label})"


def build_consolidated_influence_labels(
    topic_to_seen_statuses: dict[str, Set[OccupancyStatus]],
) -> list[str]:
    """For a mapping of influence topics to sets of seen statuses for their occupancies,
    build human-readable influence labels for each of them."""
    formatted = [
        format_influence_label(topic, get_best_influence_status(seen_statuses))
        for topic, seen_statuses in topic_to_seen_statuses.items()
    ]
    return [f for f in formatted if f is not None]


def analyze_position(context: Context, entity: Entity) -> Set[str]:
    """Analyze a position entity and emit the categorisation."""
    topics: Set[str] = set()

    # Skip if this is the only dataset containing this Position. This should be implicit
    # from other conditions, but let's be sure this dataset doesn't feed itself
    # removed Positions.
    if entity.datasets == {context.dataset.name}:
        return topics

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

        if not entity.schema.is_a("Person") or "role.pep" not in entity.get("topics"):
            continue

        # Skip if this is the only dataset containing this PEP. This should be implicit
        # from other conditions, but let's be sure this dataset doesn't feed itself
        # removed PEPs.
        if entity.datasets == {context.dataset.name}:
            continue

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

                for topic in topics:
                    if topic not in INFLUENCE_TOPIC_LABELS:
                        continue
                    topic_to_seen_statuses[topic].add(
                        get_best_occupancy_status(occupancy)
                    )

        # For each topic, we determine the best status seen and build the label from it.
        influence_labels = build_consolidated_influence_labels(topic_to_seen_statuses)
        if not influence_labels:
            continue
        person_proxy = context.make("Person")
        person_proxy.id = entity.id
        person_proxy.add("classification", influence_labels)
        context.emit(person_proxy)
