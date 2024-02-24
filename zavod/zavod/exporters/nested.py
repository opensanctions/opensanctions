from zavod.exporters.common import Exporter
from zavod.util import write_json
from zavod.entity import Entity


class NestedJSONExporter(Exporter):
    def setup(self) -> None:
        super().setup()
        self.fh = open(self.path, "wb")

    def is_root(self, entity: Entity) -> bool:
        return False

    def feed(self, entity: Entity) -> None:
        if self.is_root(entity):
            return
        data = entity.to_nested_dict(self.view)
        write_json(data, self.fh)

    def finish(self) -> None:
        self.fh.close()
        super().finish()


class NestedTargetsJSONExporter(NestedJSONExporter):
    TITLE = "Targets as nested JSON"
    FILE_NAME = "targets.nested.json"
    MIME_TYPE = "application/json"

    def is_root(self, entity: Entity) -> bool:
        return entity.target or False


class NestedTopicsJSONExporter(NestedJSONExporter):
    TITLE = "Relevant topic tagged entities as nested JSON"
    FILE_NAME = "topics.nested.json"
    MIME_TYPE = "application/json"

    _TOPICS = set(
        [
            "role.pep",
            "role.rca",
            "sanction",
            "sanction.linked",
            "debarment",
            "poi",
            "crime",
            "crime.theft",
            "crime.war",
            "crime.fin",
            "crime.traffick" "corp.disqual",
            "export.control",
            "role.oligarch",
        ]
    )

    def is_root(self, entity: Entity) -> bool:
        topics = entity.get("topics", quiet=True)
        return len(self._TOPICS.intersection(topics)) > 0
