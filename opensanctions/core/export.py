import csv
import json
import structlog
from banal import is_mapping
from datetime import date, datetime
from followthemoney import model
from followthemoney.types import registry

from opensanctions import settings, __version__
from opensanctions.model import db, Issue
from opensanctions.core.entity import Entity
from opensanctions.core.dataset import Dataset
from opensanctions.util import jointext

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


def write_object(stream, obj, indent=None):
    """Write an object for line-based JSON format."""
    data = json.dumps(obj, sort_keys=True, indent=indent, cls=JSONEncoder)
    stream.write(data + "\n")


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


def nested_entity(entity, entities, inverted, path):
    data = entity.to_dict()
    properties = data.pop("properties", {})
    for prop in entity.iterprops():
        if prop.type != registry.entity:
            continue
        values = properties.pop(prop.name)
        if prop in path:
            continue
        nested = []
        for value in values:
            adjacent = entities.get(value)
            sub_path = path + [prop]
            sub = nested_entity(adjacent, entities, inverted, sub_path)
            nested.append(sub)
        properties[prop.name] = nested

    for prop, ref in inverted.get(entity.id, []):
        if prop in path:
            continue
        adjacent = entities.get(ref)
        sub_path = path + [prop]
        sub = nested_entity(adjacent, entities, inverted, sub_path)
        if prop.reverse.name not in properties:
            properties[prop.reverse.name] = []
        properties[prop.reverse.name].append(sub)

    data["properties"] = properties
    return data


def _prefix(*parts):
    return jointext(*parts, sep=".")


def flatten_row(nested, prefix=None):
    yield (_prefix(prefix, "id"), nested.get("id"))
    yield (_prefix(prefix, "schema"), nested.get("schema"))
    yield (_prefix(prefix, "target"), nested.get("target"))
    for prop, values in nested.get("properties").items():
        for idx, value in enumerate(values):
            prop_prefix = _prefix(prefix, prop, idx)
            if is_mapping(value):
                yield from flatten_row(value, prefix=prop_prefix)
            else:
                yield (prop_prefix, value)


def export_dataset(context, dataset):
    """Dump the contents of the dataset to the output directory."""
    ftm_path = context.get_resource_path("entities.ftm.json")
    ftm_path.parent.mkdir(exist_ok=True, parents=True)
    context.log.info("Writing entities to FtM", path=ftm_path)
    inverted = {}
    entities = {}
    with open(ftm_path, "w") as fh:
        for entity in Entity.query(dataset):
            entities[entity.id] = entity
            for prop, value in entity.itervalues():
                if prop.type != registry.entity:
                    continue
                if value not in inverted:
                    inverted[value] = []
                inverted[value].append((prop, entity.id))
            write_object(fh, entity)
    title = "FollowTheMoney entity graph"
    context.export_resource(ftm_path, mime_type="application/json+ftm", title=title)

    targets_path = context.get_resource_path("targets.json")
    targets_path.parent.mkdir(exist_ok=True, parents=True)
    context.log.info("Writing targets to nested JSON", path=targets_path)
    columns = set()
    with open(targets_path, "w") as fh:
        for entity in entities.values():
            if not entity.target:
                continue
            data = nested_entity(entity, entities, inverted, [])
            for column, _ in flatten_row(data):
                columns.add(column)
            write_object(fh, data, indent=2)

    title = "List of targets with details"
    # context.export_resource(targets_path, mime_type="application/json", title=title)

    wide_path = context.get_resource_path("wide.csv")
    wide_path.parent.mkdir(exist_ok=True, parents=True)
    context.log.info("Writing targets to wide-format CSV", path=wide_path)
    with open(wide_path, "w") as fh:
        writer = csv.writer(fh, dialect=csv.unix_dialect)
        writer.writerow(list(columns))
        for entity in entities.values():
            if not entity.target:
                continue
            data = nested_entity(entity, entities, inverted, [])
            data = dict(flatten_row(data))
            row = [data.get(c) for c in columns]
            writer.writerow(row)

    title = "List of targets with details"
    # context.export_resource(wide_path, mime_type="text/csv", title=title)

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
