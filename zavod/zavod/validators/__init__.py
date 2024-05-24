from typing import List, Type
from followthemoney.types import registry

from zavod.archive import dataset_data_path
from zavod.context import Context
from zavod.exc import RunFailedException
from zavod.meta.dataset import Dataset
from zavod.store import View
from zavod.entity import Entity
from zavod.validators.assertions import AssertionsValidator
from zavod.validators.common import BaseValidator


class DanglingReferencesValidator(BaseValidator):
    """Warns if an entity references an entity that is not in the store."""

    def feed(self, entity: Entity) -> None:
        for prop in entity.iterprops():
            if prop.type != registry.entity:
                continue
            for other_id in entity.get(prop):
                if self.view.has_entity(other_id):
                    continue
                self.context.log.warning(
                    f"{entity.id} property {prop.name} references missing id {other_id}"
                )


# FollowTheMoney prevents direct self-references so we check 1 level deep
class SelfReferenceValidator(BaseValidator):
    """Info level log if an entity references itself via one adjacent entity."""

    def feed(self, entity: Entity) -> None:
        if not entity.schema.is_a("Thing"):
            return
        for prop, other in self.view.get_adjacent(entity):
            for other_prop in other.iterprops():
                if other_prop.type != registry.entity:
                    continue
                if other_prop.reverse == prop:
                    continue
                if entity.id in other.get(other_prop):
                    self.context.log.info(
                        f"{entity.id} references itself via {prop.name} -> {other.id} -> {other_prop.name}"
                    )


class TopiclessTargetValidator(BaseValidator):
    """Warns if a target entity has no topics."""

    def feed(self, entity: Entity) -> None:
        if entity.target and not entity.get("topics"):
            self.context.log.warning(
                f"{entity.id} is a target but has no topics", entity=entity
            )


class EmptyValidator(BaseValidator):
    """Aborts if no entities are validated."""

    def __init__(self, context: Context, view: View):
        super().__init__(context, view)
        self.abort = True

    def feed(self, entity: Entity) -> None:
        self.abort = False

    def finish(self) -> None:
        if self.abort:
            self.context.log.error("No entities validated.")


VALIDATORS: List[Type[BaseValidator]] = [
    DanglingReferencesValidator,
    SelfReferenceValidator,
    TopiclessTargetValidator,
    AssertionsValidator,
    EmptyValidator,
]


def validate_dataset(dataset: Dataset, view: View) -> None:
    """
    Run all validators on the given view.

    Returns True if publication should be aborted.
    """
    try:
        context = Context(dataset)
        context.begin(clear=False)
        context.log.info(
            "Validating dataset",
            dataset=dataset_data_path(dataset.name),
        )

        validators = [validator(context, view) for validator in VALIDATORS]
        for idx, entity in enumerate(view.entities()):
            if idx > 0 and idx % 10000 == 0:
                context.log.info("Validated %s entities..." % idx, dataset=dataset.name)

            for validator in validators:
                validator.feed(entity)

        abort = False
        for validator in validators:
            validator.finish()
            if validator.abort:
                abort = True

        if abort:
            raise RunFailedException("Validation caused abort.")

    finally:
        context.close()
