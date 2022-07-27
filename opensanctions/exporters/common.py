from nomenklatura.loader import Loader

from opensanctions.core import Context, Dataset, Entity


class Exporter(object):
    """A common interface for file format exports at the end of the export pipeline."""

    NAME = ""
    TITLE = ""
    EXTENSION = ""
    MIME_TYPE = "text/plain"

    def __init__(self, context: Context, loader: Loader[Dataset, Entity]):
        self.context = context
        self.dataset = context.dataset
        self.resource_name = f"{self.NAME}.{self.EXTENSION}"
        self.path = context.get_resource_path(self.resource_name)
        self.path.parent.mkdir(exist_ok=True, parents=True)
        self.loader = loader

    def setup(self):
        pass

    def feed(self, entity: Entity):
        raise NotImplemented

    def finish(self):
        resource = self.context.export_resource(
            self.path, mime_type=self.MIME_TYPE, title=self.TITLE
        )
        if resource is None:
            self.context.log.warning(
                "Export is empty: %s" % self.TITLE,
                path=self.path,
            )
            return
        self.context.log.info(
            "Exported: %s" % self.TITLE,
            path=self.path,
            size=resource["size"],
        )
