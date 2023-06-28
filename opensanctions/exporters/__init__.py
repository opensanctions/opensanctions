from typing import List, Type
from zavod.logs import get_logger

from opensanctions.core import Context, Dataset
from opensanctions.core.store import View, get_store
from opensanctions.core.issues import all_issues
from opensanctions.core.db import engine_tx
from opensanctions.core.resources import clear_resources
from opensanctions.exporters.common import Exporter, EXPORT_CATEGORY
from opensanctions.exporters.ftm import FtMExporter
from opensanctions.exporters.nested import NestedJSONExporter
from opensanctions.exporters.names import NamesExporter
from opensanctions.exporters.simplecsv import SimpleCSVExporter
from opensanctions.exporters.senzing import SenzingExporter
from opensanctions.exporters.statistics import StatisticsExporter
from opensanctions.exporters.metadata import export_metadata, dataset_to_index
from opensanctions.exporters.statements import export_statements
from opensanctions.util import write_json

log = get_logger(__name__)

EXPORTERS: List[Type[Exporter]] = [
    FtMExporter,
    NestedJSONExporter,
    NamesExporter,
    StatisticsExporter,
    SimpleCSVExporter,
    SenzingExporter,
]

__all__ = ["export_dataset", "export_metadata", "export_statements"]


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
        with engine_tx() as conn:
            clear_resources(conn, dataset, category=EXPORT_CATEGORY)
            issues = list(all_issues(conn, dataset))
        export_data(context, view)

        # Export list of data issues from crawl stage
        issues_path = context.get_resource_path("issues.json")
        context.log.info("Writing dataset issues list", path=issues_path)
        with open(issues_path, "wb") as fh:
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
    scope = Dataset.require(scope_name)
    store = get_store(scope)
    exports = scope.datasets if recurse else [scope]
    for dataset_ in exports:
        view = store.view(dataset_)
        export_dataset(dataset_, view)
