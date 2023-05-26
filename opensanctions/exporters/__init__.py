from typing import List, Type
from zavod.logs import get_logger
from nomenklatura.loader import Loader
from nomenklatura.publish.dates import simplify_dates

from opensanctions.core import Context, Dataset, Entity
from opensanctions.core.loader_disk import Database
from opensanctions.core.issues import all_issues
from opensanctions.core.db import engine_tx
from opensanctions.core.resources import clear_resources
from opensanctions.core.resolver import get_resolver
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


def assemble(entity: Entity) -> Entity:
    return simplify_dates(entity)


def export_data(context: Context, loader: Loader[Dataset, Entity]):
    clazzes = EXPORTERS
    if not context.dataset.export:
        clazzes = [StatisticsExporter]
    exporters = [clz(context, loader) for clz in clazzes]
    log.info(
        "Exporting dataset...",
        dataset=context.dataset.name,
        exporters=len(exporters),
    )

    for exporter in exporters:
        exporter.setup()

    for idx, entity in enumerate(loader):
        if idx > 0 and idx % 50000 == 0:
            log.info("Exported %s entities..." % idx, dataset=context.dataset.name)
        for exporter in exporters:
            exporter.feed(entity)

    for exporter in exporters:
        exporter.finish()


def export_dataset(dataset: Dataset, database: Database):
    """Dump the contents of the dataset to the output directory."""
    try:
        context = Context(dataset)
        with engine_tx() as conn:
            clear_resources(conn, dataset, category=EXPORT_CATEGORY)
        loader = database.view(dataset, assemble)
        export_data(context, loader)
        context.commit()

        # Export list of data issues from crawl stage
        issues_path = context.get_resource_path("issues.json")
        context.log.info("Writing dataset issues list", path=issues_path)
        with open(issues_path, "wb") as fh:
            data = {"issues": list(all_issues(context.data_conn, dataset))}
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
    resolver = get_resolver()
    database = Database(scope, resolver, cached=True)
    database.view(scope)
    exports = scope.datasets if recurse else [scope]
    for dataset_ in exports:
        export_dataset(dataset_, database)
