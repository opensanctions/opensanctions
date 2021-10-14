import structlog
from urllib.parse import urljoin
from followthemoney import model
from followthemoney.helpers import name_entity, remove_prefix_dates
from followthemoney.helpers import simplify_provenance

from opensanctions import settings
from opensanctions.model import db, Issue, Statement
from opensanctions.core import Context, Dataset
from opensanctions.core.loader import DatasetMemoryLoader
from opensanctions.exporters.common import write_json
from opensanctions.exporters.ftm import FtMExporter
from opensanctions.exporters.nested import NestedJSONExporter
from opensanctions.exporters.names import NamesExporter
from opensanctions.exporters.simplecsv import SimpleCSVExporter

# from opensanctions.exporters.widecsv import WideCSVExporter

log = structlog.get_logger(__name__)

EXPORTERS = [
    FtMExporter,
    NestedJSONExporter,
    NamesExporter,
    SimpleCSVExporter,
    # WideCSVExporter,
]


def export_global_index():
    """Export the global index for all datasets."""
    datasets = []
    for dataset in Dataset.all():
        datasets.append(dataset.to_index())

    issues_path = settings.DATASET_PATH.joinpath("issues.json")
    log.info("Writing global issues list", path=issues_path)
    with open(issues_path, "w", encoding=settings.ENCODING) as fh:
        data = {"issues": Issue.query().all()}
        write_json(data, fh)

    index_path = settings.DATASET_PATH.joinpath("index.json")
    log.info("Writing global index", datasets=len(datasets), path=index_path)
    with open(index_path, "w", encoding=settings.ENCODING) as fh:
        meta = {
            "datasets": datasets,
            "run_time": settings.RUN_TIME,
            "dataset_url": settings.DATASET_URL,
            "issues_url": urljoin(settings.DATASET_URL, "issues.json"),
            "model": model,
            "schemata": [s for s, in Statement.all_schemata()],
            "app": "opensanctions",
            "version": settings.VERSION,
        }
        write_json(meta, fh)


def export_loader(context):
    loader = DatasetMemoryLoader(context.dataset, context.resolver)
    for entity in loader.entities.values():
        entity = simplify_provenance(entity)
        entity = remove_prefix_dates(entity)
        entity = name_entity(entity)
    return loader


def export_dataset(dataset):
    """Dump the contents of the dataset to the output directory."""
    context = Context(dataset)
    context.bind()
    loader = export_loader(context)
    exporters = [Exporter(context, loader) for Exporter in EXPORTERS]
    for entity in loader:
        for exporter in exporters:
            exporter.feed(entity)

    for exporter in exporters:
        exporter.finish()

    # Make sure the exported resources are visible in the database
    db.session.commit()

    # Export list of data issues from crawl stage
    issues_path = context.get_resource_path("issues.json")
    context.log.info("Writing dataset issues list", path=issues_path)
    with open(issues_path, "w", encoding=settings.ENCODING) as fh:
        data = {"issues": Issue.query(dataset=dataset).all()}
        write_json(data, fh)

    # Export full metadata
    index_path = context.get_resource_path("index.json")
    context.log.info("Writing dataset index", path=index_path)
    with open(index_path, "w", encoding=settings.ENCODING) as fh:
        meta = dataset.to_index()
        write_json(meta, fh)

    context.close()
