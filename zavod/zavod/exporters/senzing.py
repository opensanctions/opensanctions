from nomenklatura.senzing import senzing_record
from typing import cast, Dict, Any

from zavod.entity import Entity
from zavod.exporters.common import Exporter
from zavod.util import write_json


class SenzingExporter(Exporter):
    TITLE = "Senzing entity format"
    NAME = "senzing"
    EXTENSION = "json"
    MIME_TYPE = "application/json+senzing"

    def setup(self) -> None:
        super().setup()
        self.fh = open(self.path, "wb")
        self.source_name = f"OS_{self.dataset.name.upper()}"
        if self.dataset.name in ("all", "default"):
            self.source_name = "OPENSANCTIONS"

    def feed(self, entity: Entity) -> None:
        record = senzing_record(self.source_name, entity, self.view)
        if record is not None:
            write_json(cast(Dict[str, Any], record), self.fh)

    def finish(self) -> None:
        self.fh.close()
        super().finish()
