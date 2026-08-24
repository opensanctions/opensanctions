from followthemoney.dataset import Version

from zavod.exporters.consolidate import consolidate_entity
from zavod.exc import RunFailedException
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
from zavod.runtime.statistics import Statistics
from zavod.validators import get_validators
from zavod.exporters.securities import SecuritiesExporter
from zavod.exporters.statements import StatementsCSVExporter
from zavod.exporters.maritime import MaritimeExporter
from zavod.exporters.delta import DeltaExporter

from zavod.exporters.fragment import ViewFragment
from zavod.exporters.metadata import DatasetVersionResult, write_dataset_index
from zavod.exporters.metadata import write_catalog, write_delta_index

log = get_logger(__name__)

# The statistics exporter is not listed here: it always runs.
DEFAULT_EXPORTERS: set[str] = {
    FtMExporter.FILE_NAME,
    NestedTargetsJSONExporter.FILE_NAME,
    NamesExporter.FILE_NAME,
    SimpleCSVExporter.FILE_NAME,
    SenzingExporter.FILE_NAME,
    DeltaExporter.FILE_NAME,
}
EXPORTERS: dict[str, type[Exporter]] = {
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


def get_exporters(context: Context, stats: Statistics) -> list[Exporter]:
    """Instantiate the exporters configured for the context's dataset."""
    exporter_names = set(context.dataset.model.exports)
    if not len(exporter_names):
        exporter_names.update(DEFAULT_EXPORTERS)
    exporter_names.discard(StatisticsExporter.FILE_NAME)
    exporters: list[Exporter] = [StatisticsExporter(context, stats)]
    for name in exporter_names:
        clazz = EXPORTERS.get(name)
        if clazz is None:
            log.error(f"No exporter found for target: {name}")
            continue
        exporters.append(clazz(context, stats))
    return exporters


def export_data(context: Context, view: View, validate: bool = True) -> None:
    stats = Statistics()
    exporters = get_exporters(context, stats)
    validators = get_validators(context, stats) if validate else []

    log.info(
        f"Exporting dataset: {context.dataset.name}...",
        exporters=len(exporters),
        validators=len(validators),
    )
    for exporter in exporters:
        exporter.setup()

    try:
        for idx, entity in enumerate(view.entities(prefetch_nested=True)):
            if idx > 0 and idx % 10000 == 0:
                log.info(f"Exported {idx} entities...", scope=context.dataset.name)

            # feed_unconsolidated must be called before consolidate_entity, because
            # consolidate_entity mutates the entity in place.
            for exporter in exporters:
                exporter.feed_unconsolidated(entity)

            entity = consolidate_entity(view.store.linker, entity)
            fragment = ViewFragment(view, entity)
            stats.observe(entity)
            for exporter in exporters:
                exporter.feed(entity, fragment)
            for validator in validators:
                validator.feed(entity, fragment)

        # Validators finish before exporters: on a validation abort, no exporter
        # may register its artifact for publication.
        abort = False
        for validator in validators:
            validator.finish()
            abort = abort or validator.abort
        if abort:
            raise RunFailedException("Validation caused abort.")

        for exporter in exporters:
            exporter.finish(view)
    finally:
        for exporter in exporters:
            exporter.close()


def export_dataset(
    dataset: Dataset, version: Version, view: View, validate: bool = True
) -> None:
    """Dump the contents of the dataset to the output directory.

    Unless `validate` is False, the dataset validators run on the same
    traversal; a fatal validation failure raises `RunFailedException` before
    any export artifact is registered for publication."""
    if validate and dataset.is_collection:
        log.info(f"Skipping validation for collection: {dataset.name}")
        validate = False
    context = Context(dataset, version=version)
    try:
        context.begin()
        export_data(context, view, validate=validate)
    finally:
        context.close()

    # Export metadata and issues (after the context is closed & flushed)
    write_delta_index(dataset, version)
    write_dataset_index(dataset, version, DatasetVersionResult.SUCCESS)
    write_catalog(dataset, version)
    log.info(f"Exported dataset: {dataset.name}")
