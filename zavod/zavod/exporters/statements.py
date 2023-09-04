from nomenklatura.statement.serialize import CSVStatementWriter

from zavod.entity import Entity
from zavod.exporters.common import Exporter


class StatementsCSVExporter(Exporter):
    TITLE = "Statement-based granular CSV"
    FILE_NAME = "statements.csv"
    MIME_TYPE = "text/csv+ftm-statements"

    def setup(self) -> None:
        super().setup()
        self.fh = open(self.path, "wb")
        self.writer = CSVStatementWriter(self.fh)

    def feed(self, entity: Entity) -> None:
        for stmt in entity.statements:
            self.writer.write(stmt)

    def finish(self) -> None:
        self.writer.close()
        super().finish()
