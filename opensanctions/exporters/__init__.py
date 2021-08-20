import structlog
from followthemoney import model

from opensanctions import settings
from opensanctions.model import db, Issue
from opensanctions.core import Entity, Context, Dataset
from opensanctions.exporters.common import write_json
from opensanctions.exporters.ftm import FtMExporter
from opensanctions.exporters.nested import NestedJSONExporter
from opensanctions.exporters.names import NamesExporter
from opensanctions.exporters.simplecsv import SimpleCSVExporter
from opensanctions.exporters.index import ExportIndex

log = structlog.get_logger(__name__)

EXPORTERS = [FtMExporter, NestedJSONExporter, NamesExporter, SimpleCSVExporter]


def export_global_index():
    """Export the global index for all datasets."""
    index_path = settings.DATASET_PATH.joinpath("index.json")
    datasets = []
    for dataset in Dataset.all():
        datasets.append(dataset.to_index())

    log.info("Writing global index", datasets=len(datasets), path=index_path)
    with open(index_path, "w", encoding=settings.ENCODING) as fh:
        meta = {
            "datasets": datasets,
            "run_time": settings.RUN_TIME,
            "dataset_url": settings.DATASET_URL,
            "model": model,
            "app": "opensanctions",
            "version": settings.VERSION,
        }
        write_json(meta, fh)


def export_dataset(dataset):
    """Dump the contents of the dataset to the output directory."""
    context = Context(dataset)
    context.bind()
    index = ExportIndex(dataset)
    exporters = [Exporter(context, index) for Exporter in EXPORTERS]
    for entity in index:
        for exporter in exporters:
            exporter.feed(entity)

    for exporter in exporters:
        exporter.finish()

    # ftm_path = context.get_resource_path("entities.ftm")
    # ftm_path.parent.mkdir(exist_ok=True, parents=True)
    # context.log.info("Writing entities to FtM", path=ftm_path)
    # inverted = {}
    # entities = {}
    # with open(ftm_path, "w", encoding=settings.ENCODING) as fh:
    #     for entity in Entity.query(dataset):
    #         entities[entity.id] = entity
    #         for prop, value in entity.itervalues():
    #             if prop.type != registry.entity:
    #                 continue
    #             if value not in inverted:
    #                 inverted[value] = []
    #             inverted[value].append((prop.reverse, entity.id))
    #         write_object(fh, entity)
    # title = "FollowTheMoney entities"
    # context.export_resource(ftm_path, mime_type="application/json+ftm", title=title)

    # targets_path = context.get_resource_path("targets.json")
    # targets_path.parent.mkdir(exist_ok=True, parents=True)
    # context.log.info("Writing targets to nested JSON", path=targets_path)
    # # columns = set()
    # with open(targets_path, "w", encoding=settings.ENCODING) as fh:
    #     for entity in entities.values():
    #         if not entity.target:
    #             continue
    #         data = nested_entity(entity, entities, inverted, [])
    #         # for column, _ in flatten_row(data):
    #         #     columns.add(column)
    #         write_object(fh, data)

    # title = "Targets as nested JSON"
    # context.export_resource(targets_path, mime_type="application/json", title=title)

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
