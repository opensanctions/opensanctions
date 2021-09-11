import structlog
from typing import Dict, Generator, Iterator, Optional, Tuple
from followthemoney.types import registry
from followthemoney.property import Property
from nomenklatura.loader import Loader, MemoryLoader
from opensanctions.core.dataset import Dataset

from opensanctions.core import Entity
from opensanctions.model import Statement

log = structlog.get_logger(__name__)


class DatasetMemoryLoader(MemoryLoader[Dataset, Entity]):
    def __init__(self, dataset: Dataset):
        entities = Entity.query(dataset)
        super().__init__(dataset, entities)


class DatabaseLoader(Loader[Dataset, Entity]):
    def __init__(self, dataset: Dataset):
        super().__init__(dataset)

    def get_entity(self, id: str) -> Optional[Entity]:
        for entity in Entity.query(self.dataset, entity_id=id):
            return entity
        return None

    def get_inverted(self, id: str) -> Generator[Tuple[Property, Entity], None, None]:
        for entity in Entity.query(self.dataset, inverted_id=id):
            for prop, value in entity.itervalues():
                if prop.type == registry.entity and value == id:
                    yield prop.reverse, entity

    def __iter__(self) -> Iterator[Entity]:
        return iter(Entity.query(self.dataset))

    def __len__(self) -> int:
        return Statement.all_entity_ids(self.dataset).count()
