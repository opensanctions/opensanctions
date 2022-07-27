from normality import normalize
from collections import defaultdict
from followthemoney.types import registry

from opensanctions.exporters.common import Exporter


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
        batch = []
        with open(self.path, "w") as fh:
            for norm in sorted(self.names):
                for name in sorted(self.names[norm]):
                    batch.append(name)

                if len(batch) > 10000:
                    text = "\n".join(batch)
                    fh.write(f"{text}\n")
                    batch = []

            if len(batch):
                text = "\n".join(batch)
                fh.write(f"{text}\n")
        super().finish()
