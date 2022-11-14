from stringcase import constcase
from nomenklatura.senzing import senzing_record

from opensanctions.core import Entity
from opensanctions.exporters.common import Exporter
from opensanctions.util import write_json


class SenzingExporter(Exporter):
    TITLE = "Senzing entity format"
    NAME = "senzing"
    EXTENSION = "json"
    MIME_TYPE = "application/json+senzing"

    def setup(self):
        super().setup()
        self.fh = open(self.path, "wb")
        self.source_name = constcase(f"OS_{self.dataset.name}")
        if self.dataset.name in ("all", "default"):
            self.source_name = "OPENSANCTIONS"

    def feed(self, entity: Entity):
        record = senzing_record(self.source_name, entity, self.loader)
        if record is not None:
            write_json(record, self.fh)

    def finish(self):
        self.fh.close()
        super().finish()
