from zavod.entity import Entity
from zavod.store import View
from zavod.context import Context


class Exporter(object):
    """A common interface for file format exports at the end of the export pipeline."""

    FILE_NAME = ""
    TITLE = ""
    MIME_TYPE = "text/plain"

    def __init__(self, context: Context, view: View):
        self.context = context
        self.dataset = context.dataset
        self.resource_name = f"{self.FILE_NAME}"
        self.path = context.get_resource_path(self.resource_name)
        self.view = view

    def setup(self) -> None:
        pass

    def feed(self, entity: Entity) -> None:
        raise NotImplementedError()

    def finish(self) -> None:
        try:
            resource = self.context.export_resource(
                self.path,
                mime_type=self.MIME_TYPE,
                title=self.TITLE,
            )
            self.context.log.info(
                "Exported: %s" % self.TITLE,
                path=self.path,
                size=resource.size,
            )
        except ValueError as ve:
            self.context.log.warning(
                "Export failed: %s" % ve,
                path=self.path,
            )
            return
