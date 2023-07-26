from followthemoney.cli.util import write_entity

from zavod.entity import Entity
from zavod.export.common import Exporter


class FtMExporter(Exporter):
    TITLE = "FollowTheMoney entities"
    NAME = "entities.ftm"
    EXTENSION = "json"
    MIME_TYPE = "application/json+ftm"

    def setup(self):
        super().setup()
        self.fh = open(self.path, "wb")

    def feed(self, entity: Entity):
        write_entity(self.fh, entity)

    def finish(self):
        self.fh.close()
        super().finish()
