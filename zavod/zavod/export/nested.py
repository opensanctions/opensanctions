from zavod.export.common import Exporter
from zavod.util import write_json


class NestedJSONExporter(Exporter):
    TITLE = "Targets as nested JSON"
    NAME = "targets.nested"
    EXTENSION = "json"
    MIME_TYPE = "application/json"

    def setup(self):
        super().setup()
        self.fh = open(self.path, "wb")

    def feed(self, entity):
        if not entity.target:
            return
        data = entity.to_nested_dict(self.view)
        write_json(data, self.fh)

    def finish(self):
        self.fh.close()
        super().finish()
