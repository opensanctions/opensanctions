from typing import List, Dict, Type

from zavod.logs import get_logger
from zavod.store import View, get_store
from zavod.context import Context
from zavod.meta import Dataset, get_catalog
from zavod.runtime.issues import DatasetIssues
from zavod.archive import dataset_resource_path, ISSUES_FILE
from zavod.exporters.common import Exporter
from zavod.exporters.ftm import FtMExporter
from zavod.exporters.nested import NestedJSONExporter
from zavod.exporters.names import NamesExporter
from zavod.exporters.simplecsv import SimpleCSVExporter
from zavod.exporters.senzing import SenzingExporter
from zavod.exporters.statistics import StatisticsExporter
from zavod.exporters.metadata import write_dataset_index
from zavod.util import write_json

log = get_logger(__name__)

EXPORTERS: Dict[str, Type[Exporter]] = {
    StatisticsExporter.FILE_NAME: StatisticsExporter,
    FtMExporter.FILE_NAME: FtMExporter,
    NestedJSONExporter.FILE_NAME: NestedJSONExporter,
    NamesExporter.FILE_NAME: NamesExporter,
    SimpleCSVExporter.FILE_NAME: SimpleCSVExporter,
    SenzingExporter.FILE_NAME: SenzingExporter,
}

__all__ = ["export_dataset", "write_dataset_index"]


def export_data(context: Context, view: View) -> None:
    exporter_names = set(context.dataset.exports)
    if not len(exporter_names):
        exporter_names.update(EXPORTERS.keys())
    exporter_names.add(StatisticsExporter.FILE_NAME)
    exporters: List[Exporter] = []
    for name in exporter_names:
        clazz = EXPORTERS.get(name)
        if clazz is None:
            log.error(f"No exporter found for target: {name}")
            continue
        exporters.append(clazz(context, view))

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


def write_issues(dataset: Dataset) -> None:
    """Export list of data issues from crawl stage."""
    issues_path = dataset_resource_path(dataset.name, ISSUES_FILE)
    log.info("Writing dataset issues list", path=issues_path.as_posix())
    with open(issues_path, "wb") as fh:
        issues = DatasetIssues(dataset)
        data = {"issues": list(issues.all())}
        write_json(data, fh)


def export_dataset(dataset: Dataset, view: View) -> None:
    """Dump the contents of the dataset to the output directory."""
    try:
        context = Context(dataset)
        context.begin(clear=False)
        export_data(context, view)

        write_issues(dataset)

        # Export full metadata
        write_dataset_index(dataset)

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
