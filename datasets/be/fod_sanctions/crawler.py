from lxml import etree
from zipfile import ZipFile

from zavod import Context
from zavod import helpers as h
from zavod.shed.fsf import parse_entry


def crawl(context: Context):
    path = context.fetch_resource("source.zip", context.data_url)
    context.export_resource(path, "application/zip", title=context.SOURCE_TITLE)
    with ZipFile(path, "r") as zip:
        for name in zip.namelist():
            if name.endswith(".xml"):
                with zip.open(name) as fh:
                    doc = etree.parse(fh)
                    doc_ = h.remove_namespace(doc)
                    for entry in doc_.findall(".//sanctionEntity"):
                        parse_entry(context, entry)
                        # print(entry, entry.get("euReferenceNumber"), entry.attrib)
