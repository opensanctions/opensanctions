from opensanctions.core import Context
from opensanctions import helpers as h
from opensanctions.crawlers.eu_fsf import parse_entry, parse_sanctions


def salvage_entity(context: Context, entry):
    texts = [t.text for t in entry.findall("./remark")]
    assert len(texts) == 2, texts
    name, details = texts
    name = name.split("(", 1)[0]
    entity = context.make("LegalEntity")
    entity.id = context.make_id(name)
    entity.add("name", name)
    entity.add("notes", details)
    entity.add("topics", "sanction")
    parse_sanctions(context, entity, entry)
    context.emit(entity, target=True)


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.dataset.data.url)
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    doc = h.remove_namespace(doc)
    for entry in doc.findall(".//sanctionEntity"):
        subject_type = entry.find("./subjectType")
        if subject_type is None:
            salvage_entity(context, entry)
            continue
        parse_entry(context, entry)
