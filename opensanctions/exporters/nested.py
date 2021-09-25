from followthemoney.types import registry

from opensanctions.exporters.common import Exporter, write_object


class NestedJSONExporter(Exporter):
    TITLE = "Targets as nested JSON"
    NAME = "targets.nested"
    EXTENSION = "json"
    MIME_TYPE = "application/json"

    def nested(self, entity, path, root=True):
        path = path + [entity.id]
        data = entity.to_dict()
        nested = {}
        for prop, adjacent in self.loader.get_adjacent(entity, inverted=root):
            if adjacent.id in path:
                continue
            value = self.nested(adjacent, path, False)
            if prop.name not in nested:
                nested[prop.name] = []
            nested[prop.name].append(value)
        data["properties"].update(nested)
        return data

    def feed(self, entity):
        if not entity.target:
            return
        data = self.nested(entity, [])
        write_object(self.fh, data)
