from typing import Any, Generator

from zavod.entity import Entity
from zavod.archive import ENTITIES_DELTA_FILE
from zavod.exporters.common import Exporter
from zavod.runtime.delta import HashDelta
from zavod.util import write_json


class DeltaExporter(Exporter):
    TITLE = "Delta files"
    FILE_NAME = ENTITIES_DELTA_FILE
    MIME_TYPE = "application/json"

    def setup(self) -> None:
        super().setup()
        self.delta = HashDelta(self.dataset)
        self.delta.backfill()

    def feed(self, entity: Entity) -> None:
        self.delta.feed(entity)

    def generate(self) -> Generator[Any, None, None]:
        for op, entity_id in self.delta.generate():
            if op == "DEL":
                yield {"op": "DEL", "entity": {"id": entity_id}}
                continue
            entity = self.view.get_entity(entity_id)
            if entity is None:  # watman
                continue
            yield {"op": op, "entity": entity.to_dict()}

    def finish(self) -> None:
        with open(self.path, "wb") as fh:
            for op in self.generate():
                write_json(op, fh)
        self.delta.close()
        super().finish()
