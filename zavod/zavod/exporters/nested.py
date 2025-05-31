from zavod.exporters.common import Exporter, ExportView
from zavod.util import write_json
from zavod.entity import Entity


class NestedTargetsJSONExporter(Exporter):
    TITLE = "Targets as nested JSON"
    FILE_NAME = "targets.nested.json"
    MIME_TYPE = "application/json"

    def setup(self) -> None:
        super().setup()
        self.fh = open(self.path, "wb")

    def feed(self, entity: Entity, view: ExportView) -> None:
        if entity.target:
            data = entity.to_nested_dict(view)
            write_json(data, self.fh)

    def finish(self, view: ExportView) -> None:
        self.fh.close()
        super().finish(view)
