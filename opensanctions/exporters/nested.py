from followthemoney.types import registry

from opensanctions.exporters.common import Exporter, write_object


class NestedJSONExporter(Exporter):
    TITLE = "Targets as nested JSON"
    NAME = "targets.nested"
    EXTENSION = "json"
    MIME_TYPE = "application/json"

    async def feed(self, entity):
        if not entity.target:
            return
        data = await entity.to_nested_dict(self.loader)
        await write_object(self.fh, data)
