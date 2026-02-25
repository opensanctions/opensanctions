from typing import Any, Generator
from zavod.entity import Entity
from zavod.archive import DELTA_EXPORT_FILE
from zavod.exporters.common import Exporter, ExportView
from zavod.exporters.consolidate import consolidate_entity
from zavod.runtime.delta import HashDelta
from zavod.util import write_json


class DeltaExporter(Exporter):
    TITLE = "Delta files"
    FILE_NAME = DELTA_EXPORT_FILE
    MIME_TYPE = "application/json"

    def setup(self) -> None:
        super().setup()
        self.delta = HashDelta(self.dataset)
        self.delta.backfill()
        self.counts = {
            "ADD": 0,
            "MOD": 0,
            "DEL": 0,
        }

    def feed(self, entity: Entity, view: ExportView) -> None:
        self.delta.feed(entity)

    def generate(self, view: ExportView) -> Generator[Any, None, None]:
        for op, entity_id in self.delta.generate():
            if op == "DEL":
                yield {"op": "DEL", "entity": {"id": entity_id}}
                continue
            entity = view.get_entity(entity_id)
            if entity is None:  # watman
                continue
            entity = consolidate_entity(view.store.linker, entity)
            yield {"op": op, "entity": entity.to_dict()}

    def finish(self, view: ExportView) -> None:
        with open(self.path, "wb") as fh:
            for op in self.generate(view):
                self.counts[op["op"]] += 1
                write_json(op, fh)
        self.delta.close()
        self.context.log.info(
            "Delta export complete",
            version=str(self.context.version),
            metric="delta_counts",
            added=self.counts["ADD"],
            modified=self.counts["MOD"],
            deleted=self.counts["DEL"],
        )

        super().finish(view)
