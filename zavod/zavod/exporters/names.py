from typing import Set
from normality import collapse_spaces
from followthemoney.types import registry

from zavod.exporters.common import Exporter
from zavod.entity import Entity

class NamesExporter(Exporter):
    TITLE = "Target names text file"
    FILE_NAME = "names.txt"
    MIME_TYPE = "text/plain"

    def setup(self) -> None:
        super().setup()
        self.fh = open(self.path, "w")
        self.seen_hashes: Set[int] = set()

    def feed(self, entity: Entity) -> None:
        for name in entity.get_type_values(registry.name):
            name_collapsed = collapse_spaces(name)
            if name_collapsed is not None:
                key = hash(name_collapsed.lower())
                if len(name_collapsed) > 3 and key not in self.seen_hashes:
                    self.seen_hashes.add(key)
                    self.fh.write(f"{name_collapsed}\n")

    def finish(self) -> None:
        self.fh.close()
        super().finish()
