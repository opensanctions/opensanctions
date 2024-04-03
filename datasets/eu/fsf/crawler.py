from zavod import Context
from zavod import helpers as h
from zavod.shed.fsf import parse_entry


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.xml", context.data_url)
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    doc_ = h.remove_namespace(doc)
    for entry in doc_.findall(".//sanctionEntity"):
        parse_entry(context, entry)
