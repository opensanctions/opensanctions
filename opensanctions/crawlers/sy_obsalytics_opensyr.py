from pantomime.types import JSON
from followthemoney.cli.util import binary_entities

from opensanctions.core import Context, Entity


def crawl(context: Context):
    path = context.fetch_resource("source.json", context.dataset.data.url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "rb") as fh:
        for entity in binary_entities(fh, Entity):
            proxy = context.make(entity.schema)
            proxy.id = entity.id
            for prop, value in entity.itervalues():
                proxy.unsafe_add(prop, value)

            context.emit(proxy, target=entity.schema.matchable)
