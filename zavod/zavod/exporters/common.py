from nomenklatura.store import View

from zavod.archive import dataset_artifact_path
from zavod.meta import Dataset
from zavod.entity import Entity
from zavod.context import Context
from zavod.runtime.statistics import Statistics

ExportView = View[Dataset, Entity]


class Exporter:
    """A common interface for file format exports at the end of the export pipeline."""

    FILE_NAME = ""
    TITLE = ""
    MIME_TYPE = "text/plain"

    def __init__(self, context: Context, stats: Statistics):
        self.context = context
        self.stats = stats
        self.dataset = context.dataset
        self.path = dataset_artifact_path(
            self.dataset.name, context.version, self.FILE_NAME
        )

    def setup(self) -> None:
        pass

    def feed(self, entity: Entity, view: ExportView) -> None:
        raise NotImplementedError()

    def feed_unconsolidated(self, entity: Entity) -> None:
        pass

    def close(self) -> None:
        """Release resources held by the exporter."""
        pass

    def finish(self, view: ExportView) -> None:
        try:
            resource = self.context.export_resource(
                self.path,
                mime_type=self.MIME_TYPE,
                title=self.TITLE,
            )
            self.context.log.info(
                f"Exported: {self.TITLE}",
                path=self.path,
                size=resource.size,
            )
        except ValueError as ve:
            self.context.log.warning(
                f"Export failed: {ve}",
                path=self.path,
            )
            return
