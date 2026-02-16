from zavod import Context
from zavod import helpers as h
from zavod.shed.fsf import parse_entry
from zavod.stateful.review import assert_all_accepted


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.xml", context.data_url)
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    doc_ = h.remove_namespace(doc)
    for entry in doc_.findall(".//sanctionEntity"):
        parse_entry(context, entry)

    # TODO: Stop raising once we're through the initial bunch of reviews.
    assert_all_accepted(context, raise_on_unaccepted=True)
