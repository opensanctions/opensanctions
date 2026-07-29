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
from zavod.exporters.metadata import DatasetVersionResult, write_dataset_index
from zavod.exporters.metadata import write_catalog, write_delta_index

log = get_logger(__name__)

DEFAULT_EXPORTERS: set[str] = {
    StatisticsExporter.FILE_NAME,
    FtMExporter.FILE_NAME,
    NestedTargetsJSONExporter.FILE_NAME,
    NamesExporter.FILE_NAME,
    SimpleCSVExporter.FILE_NAME,
    SenzingExporter.FILE_NAME,
    DeltaExporter.FILE_NAME,
}
EXPORTERS: dict[str, type[Exporter]] = {
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

__all__ = ["export_dataset", "write_dataset_index", "get_exporter_names"]


def get_exporter_names(dataset: Dataset) -> set[str]:
    """The file names of the exporters enabled for the given dataset.

    A dataset which doesn't configure `exports:` runs the default set. The
    statistics exporter always runs, because the dataset metadata is derived
    from its output.
    """
    names = set(dataset.model.exports)
    if not len(names):
        names.update(DEFAULT_EXPORTERS)
    names.add(StatisticsExporter.FILE_NAME)
    return names


def export_data(context: Context, view: View) -> None:
    exporter_names = get_exporter_names(context.dataset)
    exporters: list[Exporter] = []
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
        if idx > 0 and idx % 10000 == 0:
            log.info(f"Exported {idx} entities...", scope=context.dataset.name)

        # feed_unconsolidated must be called before consolidate_entity, because
        # consolidate_entity mutates the entity in place.
        for exporter in exporters:
            exporter.feed_unconsolidated(entity)

        entity = consolidate_entity(view.store.linker, entity)
        fragment = ViewFragment(view, entity)
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
    # The delta index is built from archived delta exports, so it must only be
    # written for datasets that still produce them - otherwise it would list
    # versions from before the exporter was turned off
    # (https://github.com/opensanctions/opensanctions/issues/5140).
    delta_enabled = DeltaExporter.FILE_NAME in get_exporter_names(dataset)
    if delta_enabled:
        write_delta_index(dataset)
    write_dataset_index(dataset, DatasetVersionResult.SUCCESS, delta_enabled)
    write_catalog(dataset)
    log.info(f"Exported dataset: {dataset.name}")
