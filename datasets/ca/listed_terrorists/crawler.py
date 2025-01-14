from rigour.mime.types import XML

from zavod import Context
from zavod import helpers as h

ALIAS_SPLITS = ["; ", ", "]


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.data_url)
    context.export_resource(path, XML, title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    doc = h.remove_namespace(doc)
    for node in doc.findall("./entry"):
        entity = context.make("Organization")
        name = node.findtext("./title")
        entity.id = context.make_slug(node.findtext("./id"), name)
        entity.add("name", name)

        link = node.find("./link").get("href")
        entity.add("sourceUrl", link)
        aliases = node.findtext("./summary")
        if aliases != "N/A":
            entity.add("alias", h.multi_split(aliases, ALIAS_SPLITS))
        entity.add("notes", node.findtext("./content"))
        entity.add("createdAt", node.findtext("./published"))
        entity.add("modifiedAt", node.findtext("./updated"))
        entity.add("topics", "crime.terror")

        context.emit(entity)
