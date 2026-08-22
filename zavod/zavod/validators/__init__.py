from collections import Counter, defaultdict
from followthemoney import registry, Property, Schema
from nomenklatura.store import View

from zavod.context import Context
from zavod.entity import Entity
from zavod.meta.dataset import Dataset
from zavod.runtime.statistics import Statistics
from zavod.validators.assertions import (
    StatisticsAssertionsValidator,
)
from zavod.validators.common import BaseValidator

# How many offending references to name per property/schema combination. Some
# datasets get this wrong thousands of times over; a handful of ids is enough to
# find the crawler code responsible.
MAX_RANGE_EXAMPLES = 5


class EntityReferenceValidator(BaseValidator):
    """Warn if an entity reference doesn't resolve, or points at the wrong kind
    of entity.

    Every entity-type property declares a `range`: the schema its target is
    supposed to have (an `Ownership:asset` must be an `Asset`, an
    `Occupancy:holder` must be a `Person`). Nothing can enforce that while the
    crawler runs, because the referenced entity usually doesn't exist yet when
    the reference is made. Once the whole dataset is in the store, it becomes
    checkable - a company emitted as an `Organization` rather than a `Company`
    shows up here.

    Enabled by default; set `validators.entity_reference` to `false` in the
    dataset metadata to switch it off.
    """

    @classmethod
    def enabled(cls, dataset: Dataset) -> bool:
        return dataset.validators.entity_reference

    def __init__(self, context: Context, stats: Statistics) -> None:
        super().__init__(context, stats)
        self.out_of_range: Counter[tuple[Property, Schema]] = Counter()
        self.examples: dict[tuple[Property, Schema], list[str]] = defaultdict(list)

    def feed(self, entity: Entity, view: View[Dataset, Entity]) -> None:
        for prop in entity.iterprops():
            if prop.type != registry.entity:
                continue
            for other_id in entity.get(prop):
                other = view.get_entity(other_id)
                if other is None:
                    self.context.log.warning(
                        f"{entity.id} property {prop.name} references missing id {other_id}"
                    )
                    continue
                if prop.range is None or other.schema.is_a(prop.range):
                    continue
                key = (prop, other.schema)
                self.out_of_range[key] += 1
                examples = self.examples[key]
                if len(examples) < MAX_RANGE_EXAMPLES:
                    examples.append(f"{entity.id} -> {other_id}")

    def finish(self) -> None:
        for (prop, schema), count in self.out_of_range.most_common():
            assert prop.range is not None
            self.context.log.warning(
                f"{prop.qname} should reference {prop.range.name}, "
                f"but {count} references point at {schema.name}",
                prop=prop.qname,
                range=prop.range.name,
                referenced_schema=schema.name,
                count=count,
                examples=self.examples[(prop, schema)],
            )


# FollowTheMoney prevents direct self-references so we check 1 level deep
class SelfReferenceValidator(BaseValidator):
    """Info level log if an entity references itself via one adjacent entity."""

    def feed(self, entity: Entity, view: View[Dataset, Entity]) -> None:
        if not entity.schema.is_a("Thing"):
            return
        for prop, other in view.get_adjacent(entity):
            for other_prop in other.iterprops():
                if other_prop.type != registry.entity:
                    continue
                if other_prop.reverse == prop:
                    continue
                if entity.id in other.get(other_prop):
                    self.context.log.info(
                        f"{entity.id} references itself via {prop.name} -> {other.id} -> {other_prop.name}"
                    )


class EmptyValidator(BaseValidator):
    """Warn if no entities are validated."""

    def __init__(self, context: Context, stats: Statistics):
        super().__init__(context, stats)
        self.is_empty = True

    def feed(self, entity: Entity, view: View[Dataset, Entity]) -> None:
        self.is_empty = False

    def finish(self) -> None:
        if self.is_empty:
            self.context.log.warning("No entities validated.")


VALIDATORS: list[type[BaseValidator]] = [
    EntityReferenceValidator,
    SelfReferenceValidator,
    StatisticsAssertionsValidator,
    EmptyValidator,
]


def get_validators(context: Context, stats: Statistics) -> list[BaseValidator]:
    """Instantiate the validators enabled for the context's dataset."""
    validators: list[BaseValidator] = []
    for clazz in VALIDATORS:
        if not clazz.enabled(context.dataset):
            continue
        validators.append(clazz(context, stats))
    return validators
