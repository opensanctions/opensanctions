from typing import List, Dict, Type, Set

from zavod.logs import get_logger
from zavod.store import View
from zavod.context import Context
from zavod.meta import Dataset
from zavod.exporters.common import Exporter
from zavod.exporters.ftm import FtMExporter
from zavod.exporters.nested import NestedTargetsJSONExporter
from zavod.exporters.nested import NestedTopicsJSONExporter
from zavod.exporters.names import NamesExporter
from zavod.exporters.simplecsv import SimpleCSVExporter
from zavod.exporters.simplecsv_02 import SimpleCSV_02Exporter
from zavod.exporters.senzing import SenzingExporter
from zavod.exporters.statistics import StatisticsExporter
from zavod.exporters.securities import SecuritiesExporter
from zavod.exporters.statements import StatementsCSVExporter
from zavod.exporters.delta import DeltaExporter
from zavod.exporters.metadata import write_dataset_index, write_issues
from zavod.exporters.metadata import write_catalog, write_delta_index

log = get_logger(__name__)

DEFAULT_EXPORTERS: Set[str] = {
    StatisticsExporter.FILE_NAME,
    FtMExporter.FILE_NAME,
    NestedTargetsJSONExporter.FILE_NAME,
    NamesExporter.FILE_NAME,
    SimpleCSVExporter.FILE_NAME,
    SenzingExporter.FILE_NAME,
    DeltaExporter.FILE_NAME,
}
EXPORTERS: Dict[str, Type[Exporter]] = {
    StatisticsExporter.FILE_NAME: StatisticsExporter,
    FtMExporter.FILE_NAME: FtMExporter,
    NestedTargetsJSONExporter.FILE_NAME: NestedTargetsJSONExporter,
    NestedTopicsJSONExporter.FILE_NAME: NestedTopicsJSONExporter,
    NamesExporter.FILE_NAME: NamesExporter,
    SimpleCSVExporter.FILE_NAME: SimpleCSVExporter,
    SimpleCSV_02Exporter.FILE_NAME: SimpleCSV_02Exporter,
    SenzingExporter.FILE_NAME: SenzingExporter,
    SecuritiesExporter.FILE_NAME: SecuritiesExporter,
    StatementsCSVExporter.FILE_NAME: StatementsCSVExporter,
    DeltaExporter.FILE_NAME: DeltaExporter,
}

__all__ = ["export_dataset", "write_dataset_index", "write_issues"]


def export_data(context: Context, view: View) -> None:
    exporter_names = set(context.dataset.exports)
    if not len(exporter_names):
        exporter_names.update(DEFAULT_EXPORTERS)
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


def export_dataset(dataset: Dataset, view: View) -> None:
    """Dump the contents of the dataset to the output directory."""
    try:
        context = Context(dataset)
        context.begin(clear=False)
        export_data(context, view)

        # Export full metadata
        write_issues(dataset)
        write_delta_index(dataset)
        write_dataset_index(dataset)
        write_catalog(dataset)
        log.info("Exported dataset: %s" % dataset.name, dataset=dataset.name)
    finally:
        context.close()
