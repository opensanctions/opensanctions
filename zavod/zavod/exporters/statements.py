from normality.encoding import DEFAULT_ENCODING
from followthemoney.statement.serialize import CSVStatementWriter

from zavod.entity import Entity
from zavod.exporters.common import Exporter, ExportView


class StatementsCSVExporter(Exporter):
    TITLE = "Statement-based granular CSV"
    FILE_NAME = "statements.csv"
    MIME_TYPE = "text/csv+ftm-statements"

    def setup(self) -> None:
        super().setup()
        self.fh = open(self.path, "w", encoding=DEFAULT_ENCODING)
        self.writer = CSVStatementWriter(self.fh)

    def feed(self, entity: Entity, view: ExportView) -> None:
        for stmt in entity.statements:
            self.writer.write(stmt)

    def finish(self, view: ExportView) -> None:
        self.writer.close()
        super().finish(view)
