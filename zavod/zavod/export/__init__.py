from typing import List, Type

from zavod.logs import get_logger
from zavod.store import View, get_store
from zavod.context import Context
from zavod.meta import Dataset, get_catalog
from zavod.export.common import Exporter
from zavod.export.ftm import FtMExporter
from zavod.export.nested import NestedJSONExporter
from zavod.export.names import NamesExporter
from zavod.export.simplecsv import SimpleCSVExporter
from zavod.export.senzing import SenzingExporter
from zavod.export.statistics import StatisticsExporter
from zavod.export.metadata import export_metadata, dataset_to_index
from zavod.util import write_json

log = get_logger(__name__)

EXPORTERS: List[Type[Exporter]] = [
    FtMExporter,
    NestedJSONExporter,
    NamesExporter,
    StatisticsExporter,
    SimpleCSVExporter,
    SenzingExporter,
]

__all__ = ["export_dataset", "export_metadata"]


def export_data(context: Context, view: View):
    clazzes = EXPORTERS
    if not context.dataset.export:
        clazzes = [StatisticsExporter]
    exporters = [clz(context, view) for clz in clazzes]
    log.info(
        "Exporting dataset...",
        dataset=context.dataset.name,
        exporters=len(exporters),
    )

    for exporter in exporters:
        exporter.setup()

    for idx, entity in enumerate(view.entities()):
        if idx > 0 and idx % 10000 == 0:
            log.info("Exported %s entities..." % idx, dataset=context.dataset.name)
        for exporter in exporters:
            exporter.feed(entity)

    for exporter in exporters:
        exporter.finish()


def export_dataset(dataset: Dataset, view: View):
    """Dump the contents of the dataset to the output directory."""
    try:
        context = Context(dataset)
        context.begin(clear=False)
        export_data(context, view)

        # Export list of data issues from crawl stage
        issues_path = context.get_resource_path("issues.json")
        context.log.info("Writing dataset issues list", path=issues_path)
        with open(issues_path, "wb") as fh:
            issues = list(context.issues.all())
            data = {"issues": issues}
            write_json(data, fh)

        # Export full metadata
        index_path = context.get_resource_path("index.json")
        context.log.info("Writing dataset index", path=index_path)
        with open(index_path, "wb") as fh:
            meta = dataset_to_index(dataset)
            write_json(meta, fh)
    finally:
        context.close()


def export(scope_name: str, recurse: bool = False) -> None:
    """Export dump files for all datasets in the given scope."""
    scope = get_catalog().require(scope_name)
    store = get_store(scope)
    exports = scope.datasets if recurse else [scope]
    for dataset_ in exports:
        view = store.view(dataset_)
        export_dataset(dataset_, view)
