from typing import Set
from normality import collapse_spaces
from followthemoney.types import registry

from zavod.exporters.common import Exporter


class NamesExporter(Exporter):
    TITLE = "Target names text file"
    NAME = "names"
    EXTENSION = "txt"
    MIME_TYPE = "text/plain"

    def setup(self):
        super().setup()
        self.fh = open(self.path, "w")
        self.seen_hashes: Set[int] = set()

    def feed(self, entity):
        for name in entity.get_type_values(registry.name):
            name = collapse_spaces(name)
            key = hash(name.lower())
            if len(name) > 3 and key not in self.seen_hashes:
                self.seen_hashes.add(key)
                self.fh.write(f"{name}\n")

    def finish(self):
        self.fh.close()
        super().finish()
