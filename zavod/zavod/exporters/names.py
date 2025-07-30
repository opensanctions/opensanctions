from typing import Set
from normality import squash_spaces
from followthemoney import registry

from zavod.exporters.common import Exporter, ExportView
from zavod.entity import Entity


class NamesExporter(Exporter):
    TITLE = "Target names text file"
    FILE_NAME = "names.txt"
    MIME_TYPE = "text/plain"

    def setup(self) -> None:
        super().setup()
        self.fh = open(self.path, "w")
        self.seen_hashes: Set[int] = set()

    def feed(self, entity: Entity, view: ExportView) -> None:
        for name in entity.get_type_values(registry.name):
            name_collapsed = squash_spaces(name)
            if len(name_collapsed) > 0:
                key = hash(name_collapsed.lower())
                if len(name_collapsed) > 3 and key not in self.seen_hashes:
                    self.seen_hashes.add(key)
                    self.fh.write(f"{name_collapsed}\n")

    def finish(self, view: ExportView) -> None:
        self.fh.close()
        super().finish(view)
