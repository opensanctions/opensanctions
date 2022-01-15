from opensanctions.exporters.common import Exporter, write_object


class FtMExporter(Exporter):
    TITLE = "FollowTheMoney entities"
    NAME = "entities.ftm"
    EXTENSION = "json"
    MIME_TYPE = "application/json+ftm"

    def feed(self, entity):
        write_object(self.fh, entity)
