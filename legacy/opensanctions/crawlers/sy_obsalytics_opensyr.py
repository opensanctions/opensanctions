from pantomime.types import JSON
from followthemoney.proxy import EntityProxy
from followthemoney.cli.util import binary_entities

from zavod import Context


def crawl(context: Context):
    path = context.fetch_resource("source.json", context.data_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "rb") as fh:
        for entity in binary_entities(fh, EntityProxy):
            proxy = context.make(entity.schema)
            proxy.id = entity.id
            for prop, value in entity.itervalues():
                proxy.unsafe_add(prop, value)

            context.emit(proxy, target=entity.schema.matchable)
