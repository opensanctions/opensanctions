from typing import List, Dict, Type, Set

from zavod.exporters.consolidate import consolidate_entity
from zavod.logs import get_logger
from zavod.store import View
from zavod.context import Context
from zavod.meta import Dataset
from zavod.exporters.common import Exporter
from zavod.exporters.ftm import FtMExporter
from zavod.exporters.nested import NestedTargetsJSONExporter
from zavod.exporters.names import NamesExporter
from zavod.exporters.simplecsv import SimpleCSVExporter
from zavod.exporters.senzing import SenzingExporter
from zavod.exporters.statistics import StatisticsExporter
from zavod.exporters.securities import SecuritiesExporter
from zavod.exporters.statements import StatementsCSVExporter
from zavod.exporters.maritime import MaritimeExporter
from zavod.exporters.delta import DeltaExporter

from zavod.exporters.fragment import ViewFragment
from zavod.exporters.metadata import write_dataset_index
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
    NamesExporter.FILE_NAME: NamesExporter,
    SimpleCSVExporter.FILE_NAME: SimpleCSVExporter,
    SenzingExporter.FILE_NAME: SenzingExporter,
    SecuritiesExporter.FILE_NAME: SecuritiesExporter,
    MaritimeExporter.FILE_NAME: MaritimeExporter,
    StatementsCSVExporter.FILE_NAME: StatementsCSVExporter,
    DeltaExporter.FILE_NAME: DeltaExporter,
}

__all__ = ["export_dataset", "write_dataset_index"]


def export_data(context: Context, view: View) -> None:
    exporter_names = set(context.dataset.model.exports)
    if not len(exporter_names):
        exporter_names.update(DEFAULT_EXPORTERS)
    exporter_names.add(StatisticsExporter.FILE_NAME)
    exporters: List[Exporter] = []
    for name in exporter_names:
        clazz = EXPORTERS.get(name)
        if clazz is None:
            log.error(f"No exporter found for target: {name}")
            continue
        exporters.append(clazz(context))

    log.info(
        f"Exporting dataset: {context.dataset.name}...",
        exporters=len(exporters),
    )
    for exporter in exporters:
        exporter.setup()

    for idx, entity in enumerate(view.entities()):
        # Use it once we figure memory explosion out
        entity = consolidate_entity(view.store.linker, entity)
        fragment = ViewFragment(view, entity)
        if idx > 0 and idx % 10000 == 0:
            log.info("Exported %s entities..." % idx, scope=context.dataset.name)
        for exporter in exporters:
            exporter.feed(entity, fragment)

    for exporter in exporters:
        exporter.finish(view)


def export_dataset(dataset: Dataset, view: View) -> None:
    """Dump the contents of the dataset to the output directory."""
    context = Context(dataset)
    try:
        context.begin(clear=False)
        export_data(context, view)
    finally:
        context.close()

    # Export metadata and issues (after the context is closed & flushed)
    write_delta_index(dataset)
    write_dataset_index(dataset)
    write_catalog(dataset)
    log.info("Exported dataset: %s" % dataset.name)
