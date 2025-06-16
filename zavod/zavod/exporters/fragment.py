from typing import Generator, List, Optional, Dict, Tuple
from nomenklatura.store import View
from followthemoney.property import Property

from zavod.meta import Dataset
from zavod.entity import Entity
from zavod.store import View as ZavodView
from zavod.exporters.consolidate import consolidate_entity


class ViewFragment(View[Dataset, Entity]):
    """This is a cached fragment of a View that allows for efficient retrieval of a small subset of entities
    from a larger View. It is useful for accelerating multiple exporters that access the same entity context.
    """

    MAX_BUFFER = 1_000

    def __init__(self, view: ZavodView, entity: Entity):
        self.view: ZavodView = view
        self.entity = entity
        self._entities: Dict[str, Optional[Entity]] = {}
        self._inverted: Dict[str, List[str]] = {}
        if entity.id is not None:
            self._entities[entity.id] = entity

    def has_entity(self, id: str) -> bool:
        return self.get_entity(id) is not None

    def get_entity(self, id: str) -> Optional[Entity]:
        if id in self._entities:
            return self._entities[id]
        entity = self.view.get_entity(id)
        if entity is not None:
            entity = consolidate_entity(self.view.store.linker, entity)
            if len(self._entities) < self.MAX_BUFFER:
                self._entities[id] = entity
        return entity

    def get_inverted(self, id: str) -> Generator[Tuple[Property, Entity], None, None]:
        if id in self._inverted:
            for inverted_id in self._inverted[id]:
                entity = self.get_entity(inverted_id)
                if entity is not None:
                    for prop, value in entity.itervalues():
                        if value == id and prop.reverse is not None:
                            yield prop.reverse, entity
        else:
            self._inverted[id] = []
            for prop, entity in self.view.get_inverted(id):
                if entity.id is None:
                    continue
                self._inverted[id].append(entity.id)
                self._entities[entity.id] = entity
                yield prop, entity

    def entities(self) -> Generator[Entity, None, None]:
        # Don't cache entities here
        yield from self.view.entities()
