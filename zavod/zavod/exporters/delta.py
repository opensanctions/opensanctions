from typing import IO, Any
from zavod.entity import Entity
from zavod.archive import DELTA_EXPORT_FILE
from zavod.exporters.common import Exporter, ExportView
from zavod.runtime.delta import HashDelta
from zavod.util import write_json


class DeltaExporter(Exporter):
    TITLE = "Delta files"
    FILE_NAME = DELTA_EXPORT_FILE
    MIME_TYPE = "application/json"

    def setup(self) -> None:
        super().setup()
        self.delta = HashDelta(self.dataset, self.context.version)
        self.delta.backfill()
        self.fh: IO[bytes] = open(self.path, "wb")
        self.counts = {
            "ADD": 0,
            "MOD": 0,
            "DEL": 0,
        }

    def _write(self, op: str, entity: dict[str, Any]) -> None:
        self.counts[op] += 1
        write_json({"op": op, "entity": entity}, self.fh)

    def feed(self, entity: Entity, view: ExportView) -> None:
        # The fed entity is already consolidated, so added and modified
        # entities are serialized straight from the traversal - no store
        # lookups happen anywhere in the delta generation.
        for op, changed in self.delta.feed(entity):
            self._write(op, changed.to_dict())

    def close(self) -> None:
        self.delta.close()
        self.fh.close()

    def finish(self, view: ExportView) -> None:
        for op, changed in self.delta.flush():
            self._write(op, changed.to_dict())
        for entity_id in self.delta.deletions():
            self._write("DEL", {"id": entity_id})
        self.fh.close()
        self.delta.close()
        self.context.log.info(
            "Delta export complete",
            version=self.context.version.id,
            metric="delta_counts",
            added=self.counts["ADD"],
            modified=self.counts["MOD"],
            deleted=self.counts["DEL"],
        )

        super().finish(view)
