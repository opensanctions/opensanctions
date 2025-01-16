from zavod.exporters.common import Exporter
from zavod.util import write_json
from zavod.entity import Entity


class NestedJSONExporter(Exporter):
    def setup(self) -> None:
        super().setup()
        self.fh = open(self.path, "wb")

    def feed(self, entity: Entity) -> None:
        if entity.target:
            data = entity.to_nested_dict(self.view)
            write_json(data, self.fh)

    def finish(self) -> None:
        self.fh.close()
        super().finish()


class NestedTargetsJSONExporter(NestedJSONExporter):
    TITLE = "Targets as nested JSON"
    FILE_NAME = "targets.nested.json"
    MIME_TYPE = "application/json"


class NestedTopicsJSONExporter(NestedJSONExporter):
    TITLE = "Relevant topic tagged entities as nested JSON"
    FILE_NAME = "topics.nested.json"
    MIME_TYPE = "application/json"
