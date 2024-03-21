from typing import List, Type
from followthemoney.types import registry

from zavod.context import Context
from zavod.meta.dataset import Dataset
from zavod.store import View
from zavod.entity import Entity
from zavod.verifiers.assertions import AssertionsVerifier
from zavod.verifiers.common import BaseVerifier


class DanglingReferencesVerifier(BaseVerifier):
    def __init__(self, context: Context, view: View) -> None:
        super().__init__(context, view)
        self.fail = False

    def feed(self, entity: Entity) -> None:
        for prop in entity.iterprops():
            if prop.type != registry.entity:
                continue
            for other_id in entity.get(prop):
                if self.view.has_entity(other_id):
                    continue
                self.context.log.error(
                    f"{entity.id} property {prop.name} references missing id {other_id}"
                )
                self.fail = True

    def finish(self) -> None:
        if self.fail:
            raise ValueError("Dangling references found")


# FollowTheMoney prevents direct self-references so we check 1 level deep
class SelfReferenceVerifier(BaseVerifier):
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
                    self.context.log.warning(
                        f"{entity.id} references itself via {prop.name} -> {other.id} -> {other_prop.name}"
                    )


class TopiclessTargetVerifier(BaseVerifier):
    def feed(self, entity: Entity) -> None:
        if entity.target and not entity.get("topics"):
            self.context.log.warning(
                f"{entity.id} is a target but has no topics", entity=entity
            )


VERIFIERS: List[Type[BaseVerifier]] = [
    DanglingReferencesVerifier,
    SelfReferenceVerifier,
    TopiclessTargetVerifier,
    AssertionsVerifier,
]


def verify_dataset(dataset: Dataset, view: View) -> None:
    try:
        context = Context(dataset)
        context.begin(clear=False)

        verifiers = [verifier(context, view) for verifier in VERIFIERS]
        for idx, entity in enumerate(view.entities()):
            if idx > 0 and idx % 10000 == 0:
                context.log.info("Verified %s entities..." % idx, dataset=dataset.name)

            for verifier in verifiers:
                verifier.feed(entity)

        for verifier in verifiers:
            verifier.finish()
    finally:
        context.close()
