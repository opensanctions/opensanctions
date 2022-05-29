import structlog
from typing import List, Type
from nomenklatura.loader import Loader

from opensanctions import settings
from opensanctions.core import Context, Dataset, Entity
from opensanctions.core.external import External
from opensanctions.core.loader import Database
from opensanctions.core.assembly import assemble
from opensanctions.core.db import engine
from opensanctions.core.issues import all_issues
from opensanctions.exporters.common import Exporter, write_json
from opensanctions.exporters.ftm import FtMExporter
from opensanctions.exporters.nested import NestedJSONExporter
from opensanctions.exporters.names import NamesExporter
from opensanctions.exporters.simplecsv import SimpleCSVExporter
from opensanctions.exporters.metadata import export_metadata, dataset_to_index
from opensanctions.exporters.statements import export_statements

log = structlog.get_logger(__name__)

EXPORTERS: List[Type[Exporter]] = [
    FtMExporter,
    NestedJSONExporter,
    NamesExporter,
    SimpleCSVExporter,
]

__all__ = ["export_dataset", "export_metadata", "export_statements"]


def export_data(context: Context, loader: Loader[Dataset, Entity]):
    exporters = [Exporter(context, loader) for Exporter in EXPORTERS]

    for exporter in exporters:
        exporter.setup()

    for entity in loader:
        for exporter in exporters:
            exporter.feed(entity)

    for exporter in exporters:
        exporter.finish()


def export_dataset(dataset: Dataset, database: Database):
    """Dump the contents of the dataset to the output directory."""
    try:
        context = Context(dataset)
        loader = database.view(dataset, assemble)
        if dataset.type != External.TYPE:
            export_data(context, loader)

        # Export list of data issues from crawl stage
        issues_path = context.get_resource_path("issues.json")
        context.log.info("Writing dataset issues list", path=issues_path)
        with engine.begin() as conn:
            with open(issues_path, "w", encoding=settings.ENCODING) as fh:
                data = {"issues": list(all_issues(conn, dataset))}
                write_json(data, fh)

        # Export full metadata
        index_path = context.get_resource_path("index.json")
        context.log.info("Writing dataset index", path=index_path)
        with open(index_path, "w", encoding=settings.ENCODING) as fh:
            meta = dataset_to_index(dataset)
            write_json(meta, fh)
    finally:
        context.close()
