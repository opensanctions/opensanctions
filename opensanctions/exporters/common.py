import json
from datetime import date, datetime
from nomenklatura.loader import Loader

from opensanctions import settings
from opensanctions.core import Context, Dataset, Entity


class Exporter(object):
    """A common interface for file format exports at the end of the export pipeline."""

    FILE_MODE = "w"

    def __init__(self, context: Context, loader: Loader[Dataset, Entity]):
        self.context = context
        self.dataset = context.dataset
        self.resource_name = f"{self.NAME}.{self.EXTENSION}"
        self.path = context.get_resource_path(self.resource_name)
        self.path.parent.mkdir(exist_ok=True, parents=True)
        self.loader = loader

    def setup(self):
        pass

    def feed(self, entity: Entity):
        raise NotImplemented

    def finish(self):
        resource = self.context.export_resource(
            self.path, mime_type=self.MIME_TYPE, title=self.TITLE
        )
        if resource is None:
            self.context.log.warning(
                "Export is empty: %s" % self.TITLE,
                path=self.path,
            )
            return
        self.context.log.info(
            "Exported: %s" % self.TITLE,
            path=self.path,
            size=resource["size"],
        )


class JSONEncoder(json.JSONEncoder):
    """This encoder will serialize all entities that have a to_dict
    method by calling that method and serializing the result."""

    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return obj.decode(settings.ENCODING)
        if isinstance(obj, set):
            return [o for o in obj]
        if hasattr(obj, "to_dict"):
            return obj.to_dict()
        return json.JSONEncoder.default(self, obj)


def write_json(data, fh):
    """Write a JSON object to the given open file handle."""
    json_data = json.dumps(data, sort_keys=True, indent=2, cls=JSONEncoder)
    fh.write(json_data)
