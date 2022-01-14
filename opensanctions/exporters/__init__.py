import aiofiles
import structlog

from opensanctions import settings
from opensanctions.core import Context, Dataset, Entity
from opensanctions.core.loader import Database
from opensanctions.core.db import engine
from opensanctions.core.issues import all_issues
from opensanctions.exporters.common import write_json
from opensanctions.exporters.ftm import FtMExporter
from opensanctions.exporters.nested import NestedJSONExporter
from opensanctions.exporters.names import NamesExporter
from opensanctions.exporters.simplecsv import SimpleCSVExporter
from opensanctions.exporters.metadata import export_metadata, dataset_to_index
from opensanctions.exporters.statements import export_statements

log = structlog.get_logger(__name__)

EXPORTERS = [
    FtMExporter,
    NestedJSONExporter,
    NamesExporter,
    SimpleCSVExporter,
]

__all__ = ["export_dataset", "export_metadata", "export_statements"]


async def export_dataset(dataset: Dataset, database: Database):
    """Dump the contents of the dataset to the output directory."""
    context = Context(dataset)
    await context.begin()
    loader = await database.view(dataset, Entity.assembler)
    exporters = [Exporter(context, loader) for Exporter in EXPORTERS]

    for exporter in exporters:
        await exporter.setup()

    async for entity in loader.entities():
        for exporter in exporters:
            await exporter.feed(entity)

    for exporter in exporters:
        await exporter.finish()

    # Export list of data issues from crawl stage
    issues_path = context.get_resource_path("issues.json")
    context.log.info("Writing dataset issues list", path=issues_path)
    async with engine.begin() as conn:
        async with aiofiles.open(issues_path, "w", encoding=settings.ENCODING) as fh:
            data = {"issues": [i async for i in all_issues(conn, dataset)]}
            await write_json(data, fh)

    # Export full metadata
    index_path = context.get_resource_path("index.json")
    context.log.info("Writing dataset index", path=index_path)
    async with aiofiles.open(index_path, "w", encoding=settings.ENCODING) as fh:
        meta = await dataset_to_index(dataset)
        await write_json(meta, fh)

    await context.close()
