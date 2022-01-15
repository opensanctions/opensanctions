from normality import normalize
from collections import defaultdict
from followthemoney.types import registry

from opensanctions.exporters.common import Exporter, write_object


class NamesExporter(Exporter):
    TITLE = "Target names text file"
    NAME = "names"
    EXTENSION = "txt"
    MIME_TYPE = "text/plain"

    def setup(self):
        super().setup()
        self.names = defaultdict(set)

    def feed(self, entity):
        for name in entity.get_type_values(registry.name):
            name = name.strip()
            if len(name) > 3:
                norm = normalize(name, ascii=True)
                self.names[norm].add(name)

    def finish(self):
        for norm in sorted(self.names):
            for name in sorted(self.names[norm]):
                self.fh.write(f"{name}\n")
        super().finish()
