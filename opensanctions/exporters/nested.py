from followthemoney.types import registry

from opensanctions.exporters.common import Exporter, write_object


class NestedJSONExporter(Exporter):
    TITLE = "Targets as nested JSON"
    NAME = "targets.nested"
    EXTENSION = "json"
    MIME_TYPE = "application/json"

    def nested(self, entity, path, root=True):
        path = path + [entity.id]
        data = {
            "$id": entity.id,
            "$schema": entity.schema.name,
            "$last_seen": entity.last_seen,
            "$first_seen": entity.first_seen,
        }
        if entity.target:
            data["$target"] = entity.target
        for prop, value in entity.itervalues():
            if prop.type == registry.entity:
                if value in path:
                    continue
                adjacent = self.index.get_entity(value)
                value = self.nested(adjacent, path, False)
            if prop.name not in data:
                data[prop.name] = []
            data[prop.name].append(value)

        if root:
            for prop, ref in self.index.get_inverted(entity.id):
                if ref in path:
                    continue
                adjacent = self.index.get_entity(ref)
                sub = self.nested(adjacent, path, False)
                if prop.name not in data:
                    data[prop.name] = []
                data[prop.name].append(sub)

        return data

    def feed(self, entity):
        if not entity.target:
            return
        data = self.nested(entity, [])
        write_object(self.fh, data)
