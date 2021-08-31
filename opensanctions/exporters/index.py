import structlog
from followthemoney.types import registry

from opensanctions.core import Entity

log = structlog.get_logger(__name__)


class ExportIndex(object):
    def __init__(self, dataset):
        self.entities = {}
        self.inverted = {}
        log.info("Generating export index...")
        for entity in Entity.query(dataset):
            self.entities[entity.id] = entity
            for prop, value in entity.itervalues():
                if prop.type != registry.entity:
                    continue
                if value not in self.inverted:
                    self.inverted[value] = []
                self.inverted[value].append((prop.reverse, entity.id))

    def get_entity(self, id):
        return self.entities.get(id)

    def get_inverted(self, id):
        return self.inverted.get(id, [])

    def get_adjacent(self, entity, inverted=True):
        for prop, value in entity.itervalues():
            if prop.type == registry.entity:
                yield prop, self.get_entity(value)

        if inverted:
            for prop, ref in self.get_inverted(entity.id):
                yield prop, self.get_entity(ref)


    def __iter__(self):
        return iter(self.entities.values())

    def __len__(self):
        return len(self.entities)
