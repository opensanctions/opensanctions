from zavod.exporters.common import Exporter
from zavod.util import write_json
from zavod.entity import Entity


class NestedJSONExporter(Exporter):
    TITLE = "Targets as nested JSON"
    FILE_NAME = "targets.nested.json"
    MIME_TYPE = "application/json"

    def setup(self) -> None:
        super().setup()
        self.fh = open(self.path, "wb")

    def feed(self, entity: Entity) -> None:
        if not entity.target:
            return
        data = entity.to_nested_dict(self.view)
        write_json(data, self.fh)

    def finish(self) -> None:
        self.fh.close()
        super().finish()
