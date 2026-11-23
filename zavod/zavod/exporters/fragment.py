from collections.abc import Generator
from followthemoney import Schema
from nomenklatura.store import View
from followthemoney.property import Property

from zavod.meta import Dataset
from zavod.entity import Entity
from zavod.logs import get_logger
from zavod.store import View as ZavodView
from zavod.exporters.consolidate import consolidate_entity

log = get_logger(__name__)


class ViewFragment(View[Dataset, Entity]):
    """This is a cached fragment of a View that allows for efficient retrieval of a small subset of entities
    from a larger View. It is useful for accelerating multiple exporters that access the same entity context.

    Every entity a fragment returns is consolidated, including the entity it
    was made for: it is given the raw entities of the store view, which the
    view shares with the export traversal, and hands out consolidated copies.
    """

    MAX_BUFFER = 1_000

    def __init__(self, view: ZavodView, entity: Entity):
        self.view: ZavodView = view
        self.entity = consolidate_entity(view.store.linker, entity)
        self._entities: dict[str, Entity | None] = {}
        self._inverted: dict[str, list[str]] = {}
        if self.entity.id is not None:
            self._entities[self.entity.id] = self.entity

    def has_entity(self, id: str) -> bool:
        return self.get_entity(id) is not None

    def get_entity(self, id: str) -> Entity | None:
        if id in self._entities:
            return self._entities[id]
        raw = self.view.get_entity(id)
        if raw is None:
            return None
        consolidated = consolidate_entity(self.view.store.linker, raw)
        if len(self._entities) < self.MAX_BUFFER:
            self._entities[id] = consolidated
        return consolidated

    def get_inverted(self, id: str) -> Generator[tuple[Property, Entity], None, None]:
        if id in self._inverted:
            for inverted_id in self._inverted[id]:
                consolidated = self.get_entity(inverted_id)
                if consolidated is not None:
                    for prop, value in consolidated.itervalues():
                        if value == id and prop.reverse is not None:
                            yield prop.reverse, consolidated
        else:
            self._inverted[id] = []
            for prop, raw in self.view.get_inverted(id):
                if raw.id is None:
                    continue
                self._inverted[id].append(raw.id)
                consolidated = consolidate_entity(self.view.store.linker, raw)
                if len(self._entities) < self.MAX_BUFFER:
                    self._entities[raw.id] = consolidated
                yield prop, consolidated

            if len(self._inverted[id]) > self.MAX_BUFFER:
                log.debug(
                    f"Adjacency list for {id} is greater than buffer limit "
                    f"({len(self._inverted[id])} entries)",
                )

    def entities(
        self,
        include_schemata: list[Schema] | None = None,
        prefetch_nested: bool = False,
    ) -> Generator[Entity, None, None]:
        # Don't cache entities here
        raise NotImplementedError("This method should not be called on a ViewFragment!")
