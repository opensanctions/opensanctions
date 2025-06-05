from followthemoney.cli.util import write_entity

from zavod.entity import Entity
from zavod.exporters.common import Exporter, ExportView


class FtMExporter(Exporter):
    TITLE = "FollowTheMoney entities"
    FILE_NAME = "entities.ftm.json"
    MIME_TYPE = "application/json+ftm"

    def setup(self) -> None:
        super().setup()
        self.fh = open(self.path, "wb")

    def feed(self, entity: Entity, view: ExportView) -> None:
        write_entity(self.fh, entity)

    def finish(self, view: ExportView) -> None:
        self.fh.close()
        super().finish(view)
