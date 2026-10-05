from zavod.entity import Entity
from zavod.archive import STATISTICS_FILE
from zavod.exporters.common import Exporter, ExportView
from zavod.util import write_json


class StatisticsExporter(Exporter):
    """Writes out the dataset statistics observed during the export traversal.

    The `Statistics` instance is fed by the export loop itself, so that the
    same numbers serve this exporter and the dataset's assertions."""

    TITLE = "Dataset statistics"
    FILE_NAME = STATISTICS_FILE
    MIME_TYPE = "application/json"

    def feed(self, entity: Entity, view: ExportView) -> None:
        pass

    def finish(self, view: ExportView) -> None:
        with open(self.path, "wb") as fh:
            write_json(self.stats.as_dict(), fh)
        super().finish(view)
