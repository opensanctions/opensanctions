from typing import Dict, List, Set

from zavod import Context, Entity
from zavod.integration import get_dataset_linker
from zavod.meta import get_multi_dataset
from zavod.stateful.positions import OccupancyStatus, categorise_many
from zavod.store import get_store


INFLUENCE_TOPICS = {
    "gov.national": "National government",
    "gov.state": "State government",
    "gov.igo": "International organization",
    "gov.muni": "Local government",
}
STATUSES = {
    OccupancyStatus.CURRENT.value: "current",
    OccupancyStatus.ENDED.value: "past",
    OccupancyStatus.UNKNOWN.value: "unknown status",
}


class Influence:
    def __init__(self) -> None:
        # influence topic -> best status
        # e.g. {"gov.national": "current"}
        self.topics: Dict[str, str] = {}

    def add(self, topics: Set[str], statuses: List[str]) -> None:
        for topic in topics:
            if topic not in INFLUENCE_TOPICS:
                continue
            seen_status = self.topics.get(topic, None)
            for status in statuses:
                match (seen_status, status):
                    case (OccupancyStatus.CURRENT.value, _):
                        return  # Current trumps all
                    case (OccupancyStatus.UNKNOWN.value, OccupancyStatus.ENDED.value):
                        return  # Unknown trumps ended
                    case _:
                        self.topics[topic] = status

    def make_keywords(self) -> List[str]:
        keywords = []
        for topic, status in self.topics.items():
            level = INFLUENCE_TOPICS.get(topic, None)
            if level is None:
                continue
            status_label = STATUSES.get(status, None)
            if status_label is None:
                continue
            keywords.append(f"{level} ({status_label})")
        return keywords


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
            influence = Influence()

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

                    influence.add(topics, occupancy.get("status"))

            keywords = influence.make_keywords()
            if not keywords:
                continue
            person_proxy = context.make("Person")
            person_proxy.id = entity.id
            person_proxy.add("keywords", keywords)
            context.emit(person_proxy)
