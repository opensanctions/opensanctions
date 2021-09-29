import csv
import structlog
from banal import is_mapping

from opensanctions.util import jointext
from opensanctions.exporters.common import Exporter

log = structlog.get_logger(__name__)


def _prefix(*parts):
    return jointext(*parts, sep=".")


def flatten_row(nested, prefix=None):
    yield (_prefix(prefix, "id"), nested.get("id"))
    yield (_prefix(prefix, "schema"), nested.get("schema"))
    yield (_prefix(prefix, "target"), nested.get("target"))
    for idx, dataset in enumerate(nested.get("datasets", [])):
        yield (_prefix(prefix, "dataset", idx), dataset)
    for prop, values in nested.get("properties").items():
        for idx, value in enumerate(values):
            prop_prefix = _prefix(prefix, prop, idx)
            if is_mapping(value):
                yield from flatten_row(value, prefix=prop_prefix)
            else:
                yield (prop_prefix, value)


class WideCSVExporter(Exporter):
    TITLE = "Targets as wide CSV"
    NAME = "targets.wide"
    EXTENSION = "csv"
    MIME_TYPE = "text/csv"

    def setup(self):
        self.writer = csv.writer(self.fh, dialect=csv.unix_dialect)
        headers = set()
        log.info("Building wide-export columns...")
        for entity in self.loader:
            nested = entity.to_nested_dict(self.loader)
            for field, _ in flatten_row(nested):
                headers.add(field)
        self.headers = sorted(headers)
        self.writer.writerow(self.headers)

    def feed(self, entity):
        nested = entity.to_nested_dict(self.loader)
        wide = dict(flatten_row(nested))
        self.writer.writerow([wide.get(c) for c in self.headers])
