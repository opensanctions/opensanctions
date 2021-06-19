import json
import structlog
from datetime import date, datetime
from followthemoney import model
from followthemoney.cli.util import write_object

from opensanctions import settings, __version__
from opensanctions.model import db, Issue
from opensanctions.core.entity import Entity
from opensanctions.core.dataset import Dataset

log = structlog.get_logger(__name__)


class JSONEncoder(json.JSONEncoder):
    """This encoder will serialize all entities that have a to_dict
    method by calling that method and serializing the result."""

    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return obj.decode("utf-8")
        if isinstance(obj, set):
            return [o for o in obj]
        if hasattr(obj, "to_dict"):
            return obj.to_dict()
        return json.JSONEncoder.default(self, obj)


def write_json(data, fh):
    """Write a JSON object to the given open file handle."""
    json.dump(data, fh, sort_keys=True, indent=2, cls=JSONEncoder)


def export_global_index():
    """Export the global index for all datasets."""
    index_path = settings.DATASET_PATH.joinpath("index.json")
    datasets = []
    for dataset in Dataset.all():
        datasets.append(dataset.to_index())

    log.info("Writing global index", datasets=len(datasets), path=index_path)
    with open(index_path, "w") as fh:
        meta = {
            "datasets": datasets,
            "run_time": settings.RUN_TIME,
            "dataset_url": settings.DATASET_URL,
            "model": model,
            "app": "opensanctions",
            "version": __version__,
        }
        write_json(meta, fh)


def export_dataset(context, dataset):
    """Dump the contents of the dataset to the output directory."""
    ftm_path = context.get_resource_path("entities.ftm.json")
    ftm_path.parent.mkdir(exist_ok=True, parents=True)
    context.log.info("Writing entities to FtM", path=ftm_path)
    with open(ftm_path, "w") as fh:
        for entity in Entity.query(dataset):
            write_object(fh, entity)
    title = "FollowTheMoney entity graph"
    context.export_resource(ftm_path, mime_type="application/json+ftm", title=title)

    # Make sure the exported resources are visible in the database
    db.session.flush()

    # Export list of data issues from crawl stage
    issues_path = context.get_resource_path("issues.json")
    context.log.info("Writing dataset issues list", path=issues_path)
    with open(issues_path, "w") as fh:
        data = {"issues": Issue.query(dataset=dataset).all()}
        write_json(data, fh)

    # Export full metadata
    index_path = context.get_resource_path("index.json")
    context.log.info("Writing dataset index", path=index_path)
    with open(index_path, "w") as fh:
        meta = dataset.to_index()
        write_json(meta, fh)
