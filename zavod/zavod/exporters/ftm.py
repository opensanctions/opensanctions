from followthemoney.cli.util import write_entity

from zavod.entity import Entity
from zavod.exporters.common import Exporter


class FtMExporter(Exporter):
    TITLE = "FollowTheMoney entities"
    FILE_NAME = "entities.ftm.json"
    MIME_TYPE = "application/json+ftm"

    def setup(self) -> None:
        super().setup()
        self.fh = open(self.path, "wb")

    def feed(self, entity: Entity) -> None:
        write_entity(self.fh, entity)

    def finish(self) -> None:
        self.fh.close()
        super().finish()
